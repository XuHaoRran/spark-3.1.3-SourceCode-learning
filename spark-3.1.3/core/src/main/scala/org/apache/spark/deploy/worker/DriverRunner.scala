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
import java.net.URI
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{DriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.deploy.StandaloneResourceUtils.prepareResourcesFile
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{DRIVER_RESOURCES_FILE, SPARK_DRIVER_PREFIX}
import org.apache.spark.internal.config.UI.UI_REVERSE_PROXY
import org.apache.spark.internal.config.Worker.WORKER_DRIVER_TERMINATE_TIMEOUT
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.{Clock, ShutdownHookManager, SystemClock, Utils}

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 * This is currently only used in standalone cluster deploy mode.
 */
private[deploy] class DriverRunner(
    conf: SparkConf,
    val driverId: String,
    val workDir: File,
    val sparkHome: File,
    val driverDesc: DriverDescription,
    val worker: RpcEndpointRef,
    val workerUrl: String,
    val workerWebUiUrl: String,
    val securityManager: SecurityManager,
    val resources: Map[String, ResourceInformation] = Map.empty)
  extends Logging {

  @volatile private var process: Option[Process] = None
  @volatile private var killed = false

  // Populated once finished
  @volatile private[worker] var finalState: Option[DriverState] = None
  @volatile private[worker] var finalException: Option[Exception] = None

  // Timeout to wait for when trying to terminate a driver.
  private val driverTerminateTimeoutMs = conf.get(WORKER_DRIVER_TERMINATE_TIMEOUT)

  // Decoupled for testing
  def setClock(_clock: Clock): Unit = {
    clock = _clock
  }

  def setSleeper(_sleeper: Sleeper): Unit = {
    sleeper = _sleeper
  }

  private var clock: Clock = new SystemClock()
  private var sleeper = new Sleeper {
    def sleep(seconds: Int): Unit = (0 until seconds).takeWhile { _ =>
      Thread.sleep(1000)
      !killed
    }
  }

  /** Starts a thread to run and manage the driver. */
  private[worker] def start() = {
    // 启动了一条线程，使用Thread来处理Driver、Executor的启动
    new Thread("DriverRunner for " + driverId) {
      override def run(): Unit = {
        var shutdownHook: AnyRef = null
        try {
          shutdownHook = ShutdownHookManager.addShutdownHook { () =>
            logInfo(s"Worker shutting down, killing driver $driverId")
            kill()
          }

          // prepare driver jars and run driver
          // 准备Driver的jar包，运行Driver
          val exitCode = prepareAndRunDriver()

          // set final state depending on if forcibly killed and process exit code
          // 设置的最终状态取决于是否强制删除，并处理退出代码
          finalState = if (exitCode == 0) {
            Some(DriverState.FINISHED)
          } else if (killed) {
            Some(DriverState.KILLED)
          } else {
            Some(DriverState.FAILED)
          }
        } catch {
          case e: Exception =>
            kill()
            finalState = Some(DriverState.ERROR)
            finalException = Some(e)
        } finally {
          if (shutdownHook != null) {
            ShutdownHookManager.removeShutdownHook(shutdownHook)
          }
        }

        // notify worker of final driver state, possible exception
        // 通知worker节点Driver的最终状态及可能的异常
        worker.send(DriverStateChanged(driverId, finalState.get, finalException))
      }
    }.start()
  }

  /** Terminate this driver (or prevent it from ever starting if not yet started) */
  private[worker] def kill(): Unit = {
    logInfo("Killing driver process!")
    killed = true
    synchronized {
      process.foreach { p =>
        val exitCode = Utils.terminateProcess(p, driverTerminateTimeoutMs)
        if (exitCode.isEmpty) {
          logWarning("Failed to terminate driver process: " + p +
              ". This process will likely be orphaned.")
        }
      }
    }
  }

  /**
   * Creates the working directory for this driver.
   * Will throw an exception if there are errors preparing the directory.
   */
  private def createWorkingDirectory(): File = {
    val driverDir = new File(workDir, driverId)
    if (!driverDir.exists() && !driverDir.mkdirs()) {
      throw new IOException("Failed to create directory " + driverDir)
    }
    driverDir
  }

  /**
   * Download the user jar into the supplied directory and return its local path.
   * Will throw an exception if there are errors downloading the jar.
   */
  private def downloadUserJar(driverDir: File): String = {
    val jarFileName = new URI(driverDesc.jarUrl).getPath.split("/").last
    val localJarFile = new File(driverDir, jarFileName)
    // 如果在一个节点上运行多个Worker，文件可能已经存在
    if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(s"Copying user jar ${driverDesc.jarUrl} to $localJarFile")
      // 到hadoop找文件
      Utils.fetchFile(
        driverDesc.jarUrl,
        driverDir,
        conf,
        securityManager,
        SparkHadoopUtil.get.newConfiguration(conf),
        System.currentTimeMillis(),
        useCache = false)
      // 验证复制成功
      if (!localJarFile.exists()) { // Verify copy succeeded
        throw new IOException(
          s"Can not find expected jar $jarFileName which should have been loaded in $driverDir")
      }
    }
    localJarFile.getAbsolutePath
  }

  private[worker] def prepareAndRunDriver(): Int = {
    // DriverRunner创建Driver在本地系统的工作目录（即Linux的文件目录
    val driverDir = createWorkingDirectory()
    // downloadUserJar方法调用了fetchFile，fetchFile借助Hadoop，从Hdfs中下载文件。
    // 我们提交文件时，将jar包上传到Hdfs上，
    // 提交一份，大家都可以从Hdfs中下载
    val localJarFilename = downloadUserJar(driverDir)
    val resourceFileOpt = prepareResourcesFile(SPARK_DRIVER_PREFIX, resources, driverDir)

    def substituteVariables(argument: String): String = argument match {
      case "{{WORKER_URL}}" => workerUrl
      case "{{USER_JAR}}" => localJarFilename
      case other => other
    }

    // config resource file for driver, which would be used to load resources when driver starts up
    val javaOpts = driverDesc.command.javaOpts ++ resourceFileOpt.map(f =>
      Seq(s"-D${DRIVER_RESOURCES_FILE.key}=${f.getAbsolutePath}")).getOrElse(Seq.empty)
    // TODO: If we add ability to submit multiple jars they should also be added here
    val builder = CommandUtils.buildProcessBuilder(driverDesc.command.copy(javaOpts = javaOpts),
      securityManager, driverDesc.mem, sparkHome.getAbsolutePath, substituteVariables)

    // add WebUI driver log url to environment
    val reverseProxy = conf.get(UI_REVERSE_PROXY)
    val workerUrlRef = UIUtils.makeHref(reverseProxy, driverId, workerWebUiUrl)
    builder.environment.put("SPARK_DRIVER_LOG_URL_STDOUT",
      s"$workerUrlRef/logPage?driverId=$driverId&logType=stdout")
    builder.environment.put("SPARK_DRIVER_LOG_URL_STDERR",
      s"$workerUrlRef/logPage?driverId=$driverId&logType=stderr")
    // 通过ProcessBuilder启动Driver ，supervise可以自动重启
    runDriver(builder, driverDir, driverDesc.supervise)
  }

  private def runDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean): Int = {
    builder.directory(baseDir)
    def initialize(process: Process): Unit = {
      // Redirect stdout and stderr to files
      // /stdout和stderr 重定向到文件
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val redactedCommand = Utils.redactCommandLineArgs(conf, builder.command.asScala.toSeq)
        .mkString("\"", "\" \"", "\"")
      val header = "Launch Command: %s\n%s\n\n".format(redactedCommand, "=" * 40)
      Files.append(header, stderr, StandardCharsets.UTF_8)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }
    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }

  private[worker] def runCommandWithRetry(
      command: ProcessBuilderLike, initialize: Process => Unit, supervise: Boolean): Int = {
    var exitCode = -1
    // Time to wait between submission retries.
    // 等待时间提交重试
    var waitSeconds = 1
    // A run of this many seconds resets the exponential back-off.
    // 运行一定秒的时间以后回退重置
    val successfulRunDuration = 5
    var keepTrying = !killed

    val redactedCommand = Utils.redactCommandLineArgs(conf, command.command)
      .mkString("\"", "\" \"", "\"")
    // runCommandWithRetry第一次不一定能申请成功，因此循环遍历重试
    // 如果supervise设置为True，exitCode为非零退出码及driver进程没有终止，我们将keepTrying设置为True，继续循环重试启动进程
    while (keepTrying) {
      logInfo("Launch Command: " + redactedCommand)

      synchronized {
        if (killed) { return exitCode }
        process = Some(command.start())
        initialize(process.get)
      }

      val processStart = clock.getTimeMillis()
      exitCode = process.get.waitFor()

      // check if attempting another run
      // 如果尝试另一个运行检查
      keepTrying = supervise && exitCode != 0 && !killed
      if (keepTrying) {
        if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000L) {
          waitSeconds = 1
        }
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        sleeper.sleep(waitSeconds)
        waitSeconds = waitSeconds * 2 // exponential back-off
      }
    }

    exitCode
  }
}

private[deploy] trait Sleeper {
  def sleep(seconds: Int): Unit
}

// Needed because ProcessBuilder is a final class and cannot be mocked
private[deploy] trait ProcessBuilderLike {
  def start(): Process
  def command: Seq[String]
}

private[deploy] object ProcessBuilderLike {
  def apply(processBuilder: ProcessBuilder): ProcessBuilderLike = new ProcessBuilderLike {
    override def start(): Process = processBuilder.start()
    override def command: Seq[String] = processBuilder.command().asScala.toSeq
  }
}
