/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.worker

import java.io._
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.deploy.StandaloneResourceUtils.prepareResourcesFile
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SPARK_EXECUTOR_PREFIX
import org.apache.spark.internal.config.UI._
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.logging.FileAppender

/**
 * Manages the execution of one executor process.
 * This is currently only used in standalone mode.
 */
private[deploy] class ExecutorRunner(
    val appId: String,
    val execId: Int,
    val appDesc: ApplicationDescription,
    val cores: Int,
    val memory: Int,
    val worker: RpcEndpointRef,
    val workerId: String,
    val webUiScheme: String,
    val host: String,
    val webUiPort: Int,
    val publicAddress: String,
    val sparkHome: File,
    val executorDir: File,
    val workerUrl: String,
    conf: SparkConf,
    val appLocalDirs: Seq[String],
    @volatile var state: ExecutorState.Value,
    val resources: Map[String, ResourceInformation] = Map.empty)
  extends Logging {

  private val fullId = appId + "/" + execId
  private var workerThread: Thread = null
  private var process: Process = null
  private var stdoutAppender: FileAppender = null
  private var stderrAppender: FileAppender = null

  // Timeout to wait for when trying to terminate an executor.
  private val EXECUTOR_TERMINATE_TIMEOUT_MS = 10 * 1000

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might
  // make sense to remove this in the future.
  private var shutdownHook: AnyRef = null

  private[worker] def start(): Unit = {
    // 创建线程，这个线程负责下载依赖的文件，并启动CoarseGrainedExecutorBackend进程，该进程单独在一个JVM上运行
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      // 调用fetchAndRunExecutor
      override def run(): Unit = { fetchAndRunExecutor() }
    }
    // 启动线程
    workerThread.start()
    // Shutdown hook that kills actors on shutdown.
    // 终止回调函数，用于杀死进程
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      // It's possible that we arrive here before calling `fetchAndRunExecutor`, then `state` will
      // be `ExecutorState.LAUNCHING`. In this case, we should set `state` to `FAILED`.
      if (state == ExecutorState.LAUNCHING || state == ExecutorState.RUNNING) {
        state = ExecutorState.FAILED
      }
      killProcess(Some("Worker shutting down")) }
  }

  /**
   * Kill executor process, wait for exit and notify worker to update resource status.
   *
   * @param message the exception message which caused the executor's death
   */
  private def killProcess(message: Option[String]): Unit = {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo("Killing process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      exitCode = Utils.terminateProcess(process, EXECUTOR_TERMINATE_TIMEOUT_MS)
      if (exitCode.isEmpty) {
        logWarning("Failed to terminate process: " + process +
          ". This process will likely be orphaned.")
      }
    }
    try {
      worker.send(ExecutorStateChanged(appId, execId, state, message, exitCode))
    } catch {
      case e: IllegalStateException => logWarning(e.getMessage(), e)
    }
  }

  /** Stop this executor runner, including killing the process it launched */
  private[worker] def kill(): Unit = {
    if (workerThread != null) {
      // the workerThread will kill the child process when interrupted
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
      try {
        ShutdownHookManager.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us */
  private[worker] def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerUrl
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case "{{APP_ID}}" => appId
    case other => other
  }

  /**
   * Download and run the executor described in our ApplicationDescription
   * fetchAndRunExecutor类似于启动Driver的过程，在启动Executor时首先构建CommandUtils.buildProcessBuilder，
   * 然后是builder.start()，退出时发送ExecutorStateChanged消息给Worker
   */
  private def fetchAndRunExecutor(): Unit = {
    try {
      val resourceFileOpt = prepareResourcesFile(SPARK_EXECUTOR_PREFIX, resources, executorDir)
      // Launch the process
      // 传入的入口类是"org.apache.spark.executor.CoarseGrainedExecutorBackend
      val arguments = appDesc.command.arguments ++ resourceFileOpt.map(f =>
        Seq("--resourcesFile", f.getAbsolutePath)).getOrElse(Seq.empty)
      val subsOpts = appDesc.command.javaOpts.map {
        Utils.substituteAppNExecIds(_, appId, execId.toString)
      }
      val subsCommand = appDesc.command.copy(arguments = arguments, javaOpts = subsOpts)
      // 在ExecutorRunner中将通过CommandUtil构建一个ProcessBuilder，
      // 调用ProcessBuilder的start方法将会以进程的方式启动org.apache.spark.executor.CoarseGrainedExecutorBackend

      // 当Worker节点中启动ExecutorRunner时，ExecutorRunner中会启动CoarseGrainedExecutorBackend进程，
      // 在CoarseGrainedExecutorBackend的onStart方法中，向Driver发出RegisterExecutor注册请求
      val builder = CommandUtils.buildProcessBuilder(subsCommand, new SecurityManager(conf),
        memory, sparkHome.getAbsolutePath, substituteVariables)
      val command = builder.command()
      val redactedCommand = Utils.redactCommandLineArgs(conf, command.asScala.toSeq)
        .mkString("\"", "\" \"", "\"")
      logInfo(s"Launch command: $redactedCommand")

      builder.directory(executorDir)
      builder.environment.put("SPARK_EXECUTOR_DIRS", appLocalDirs.mkString(File.pathSeparator))
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      // Add webUI log urls
      val baseUrl =
        if (conf.get(UI_REVERSE_PROXY)) {
          conf.get(UI_REVERSE_PROXY_URL.key, "").stripSuffix("/") +
            s"/proxy/$workerId/logPage/?appId=$appId&executorId=$execId&logType="
        } else {
          s"$webUiScheme$publicAddress:$webUiPort/logPage/?appId=$appId&executorId=$execId&logType="
        }
      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")

      process = builder.start()
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        redactedCommand, "=" * 40)

      // Redirect its stdout and stderr to files
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf, true)

      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, StandardCharsets.UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf, true)

      state = ExecutorState.RUNNING
      worker.send(ExecutorStateChanged(appId, execId, state, None, None))
      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code
      // 等待它退出:执行器可以退出代码0(当driver 指示它关闭)或非零退出码
      val exitCode = process.waitFor()
      state = ExecutorState.EXITED
      val message = "Command exited with code " + exitCode
      // 退出时发送ExecutorStateChanged消息给Worker
      worker.send(ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode)))
    } catch {
      case interrupted: InterruptedException =>
        logInfo("Runner thread for executor " + fullId + " interrupted")
        state = ExecutorState.KILLED
        killProcess(None)
      case e: Exception =>
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        killProcess(Some(e.toString))
    }
  }
}
