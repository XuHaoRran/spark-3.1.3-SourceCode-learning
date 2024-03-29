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

package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.json4s.DefaultFormats

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.resource.ResourceProfile._
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorLossReason, TaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, SignalUtils, ThreadUtils, Utils}

private[spark] class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    bindAddress: String,
    hostname: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv,
    resourcesFileOpt: Option[String],
    resourceProfile: ResourceProfile)
  extends IsolatedRpcEndpoint with ExecutorBackend with Logging {

  import CoarseGrainedExecutorBackend._

  private implicit val formats = DefaultFormats

  private[executor] val stopping = new AtomicBoolean(false)
  var executor: Executor = null
  @volatile var driver: Option[RpcEndpointRef] = None

  // If this CoarseGrainedExecutorBackend is changed to support multiple threads, then this may need
  // to be changed so that we don't share the serializer instance across threads
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()

  private var _resources = Map.empty[String, ResourceInformation]

  /**
   * Map each taskId to the information about the resource allocated to it, Please refer to
   * [[ResourceInformation]] for specifics.
   * Exposed for testing only.
   *
   * 把每个taskId映射到有关分配给它的资源的信息
   */
  private[executor] val taskResources = new mutable.HashMap[Long, Map[String, ResourceInformation]]

  private var decommissioned = false

  // 当Worker节点中启动ExecutorRunner时，ExecutorRunner中会启动CoarseGrainedExecutorBackend进程，
  // 在CoarseGrainedExecutorBackend的onStart方法中，向Driver发出RegisterExecutor注册请求
  override def onStart(): Unit = {
    if (env.conf.get(DECOMMISSION_ENABLED)) {
      logInfo("Registering PWR handler to trigger decommissioning.")
      SignalUtils.register("PWR", "Failed to register SIGPWR handler - " +
        "disabling executor decommission feature.") (self.askSync[Boolean](ExecutorSigPWRReceived))
    }

    logInfo("Connecting to driver: " + driverUrl)
    try {
      _resources = parseOrFindResources(resourcesFileOpt)
    } catch {
      case NonFatal(e) =>
        exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
    }
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
        // 将向Driver端发送RegisterExecutor消息请求注册，完成注册后
      driver = Some(ref)
      // 向Driver发送ask请求，等待Driver回应
      ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls,
        extractAttributes, _resources, resourceProfile.id))
    }(ThreadUtils.sameThread).onComplete {
      case Success(_) =>
        self.send(RegisteredExecutor)
      case Failure(e) =>
        exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
    }(ThreadUtils.sameThread)
  }

  /**
   * Create a classLoader for use for resource discovery. The user could provide a class
   * as a substitute for the default one so we have to be able to load it from a user specified
   * jar.
   */
  private def createClassLoader(): MutableURLClassLoader = {
    val currentLoader = Utils.getContextOrSparkClassLoader
    val urls = userClassPath.toArray
    if (env.conf.get(EXECUTOR_USER_CLASS_PATH_FIRST)) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  // visible for testing
  def parseOrFindResources(resourcesFileOpt: Option[String]): Map[String, ResourceInformation] = {
    // use a classloader that includes the user classpath in case they specified a class for
    // resource discovery
    val urlClassLoader = createClassLoader()
    logDebug(s"Resource profile id is: ${resourceProfile.id}")
    Utils.withContextClassLoader(urlClassLoader) {
      val resources = getOrDiscoverAllResourcesForResourceProfile(
        resourcesFileOpt,
        SPARK_EXECUTOR_PREFIX,
        resourceProfile,
        env.conf)
      logResourceInfo(SPARK_EXECUTOR_PREFIX, resources)
      resources
    }
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase(Locale.ROOT), e._2)).toMap
  }

  def extractAttributes: Map[String, String] = {
    val prefix = "SPARK_EXECUTOR_ATTRIBUTE_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toUpperCase(Locale.ROOT), e._2)).toMap
  }


  override def receive: PartialFunction[Any, Unit] = {
    // 成功注册至driver
    case RegisteredExecutor =>

      logInfo("Successfully registered with driver")
      try {
        // 收到RegisteredExecutor消息，立即创建Executor
        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false,
          resources = _resources)
        driver.get.send(LaunchedExecutor(executorId))
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
      }
    // CoarseGrainedExecutorBackend收到Driver发送的LaunchTask任务消息，其中LaunchTask是case class，
    // 而不是case object，是因为每个消息是一个消息实例，每个消息状态不一样，而case object是唯一的，因此使用case class
    // Driver节点到executors节点
    case LaunchTask(data) =>
      if (executor == null) {
        // 先判断Executor是否为空，Executor为空，则提示错误，进程就直接退出
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else {
        // launchTask中调用了decode方法，解析读取dataIn、taskId、attemptNumber、executorId、name、index等信息，
        // 读取相应的JAR、文件、属性，返回TaskDescription值。
        val taskDesc = TaskDescription.decode(data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        taskResources(taskDesc.taskId) = taskDesc.resources
        // 创建一个TaskRunner实例在threadPool来运行具体的Task
        executor.launchTask(this, taskDesc)
      }

    case KillTask(taskId, _, interruptThread, reason) =>
      if (executor == null) {
        exitExecutor(1, "Received KillTask command but executor was null")
      } else {
        executor.killTask(taskId, interruptThread, reason)
      }

    case StopExecutor =>
      stopping.set(true)
      logInfo("Driver commanded a shutdown")
      // Cannot shutdown here because an ack may need to be sent back to the caller. So send
      // a message to self to actually do the shutdown.
      self.send(Shutdown)

    case Shutdown =>
      stopping.set(true)
      new Thread("CoarseGrainedExecutorBackend-stop-executor") {
        override def run(): Unit = {
          // `executor` can be null if there's any error in `CoarseGrainedExecutorBackend.onStart`
          // or fail to create `Executor`.
          if (executor == null) {
            System.exit(1)
          } else {
            // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
            // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
            // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
            // Therefore, we put this line in a new thread.
            executor.stop()
          }
        }
      }.start()

    case UpdateDelegationTokens(tokenBytes) =>
      logInfo(s"Received tokens of ${tokenBytes.length} bytes")
      SparkHadoopUtil.get.addDelegationTokens(tokenBytes, env.conf)

    case DecommissionExecutor =>
      decommissionSelf()
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ExecutorSigPWRReceived =>
      var driverNotified = false
      try {
        driver.foreach { driverRef =>
          // Tell driver that we are starting decommissioning so it stops trying to schedule us
          driverNotified = driverRef.askSync[Boolean](ExecutorDecommissioning(executorId))
          if (driverNotified) decommissionSelf()
        }
      } catch {
        case e: Exception =>
          if (driverNotified) {
            logError("Fail to decommission self (but driver has been notified).", e)
          } else {
            logError("Fail to tell driver that we are starting decommissioning", e)
          }
          decommissioned = false
      }
      context.reply(decommissioned)
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (stopping.get()) {
      logInfo(s"Driver from $remoteAddress disconnected during shutdown")
    } else if (driver.exists(_.address == remoteAddress)) {
      exitExecutor(1, s"Driver $remoteAddress disassociated! Shutting down.", null,
        notifyDriver = false)
    } else {
      logWarning(s"An unknown ($remoteAddress) driver disconnected.")
    }
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = {
    val resources = taskResources.getOrElse(taskId, Map.empty[String, ResourceInformation])
    val msg = StatusUpdate(executorId, taskId, state, data, resources)
    if (TaskState.isFinished(state)) {
      taskResources.remove(taskId)
    }
    // 匹配driver，如果有driver，即CoarseGrainedSchedulerBackend，就发信息，否则报错没有driver的信息
    driver match {
      // 发送信息, CoarseGrainedSchedulerBackend.receive
      case Some(driverRef) => driverRef.send(msg)
      case None => logWarning(s"Drop $msg because has not yet connected to driver")
    }
  }

  /**
   * This function can be overloaded by other child classes to handle
   * executor exits differently. For e.g. when an executor goes down,
   * back-end may not want to take the parent process down.
   *
   * CoarseGrainedExecutorBackend在运行中一旦出现异常情况，将调用exitExecutor方法处理。
   * 1.Executor向Driver注册RegisterExecutor失败。
   * 2.Executor收到Driver的RegisteredExecutor注册成功消息以后，创建Executor实例失败。
   * 3.Driver返回Executor注册失败消息RegisterExecutorFailed。
   * 4.Executor收到Driver的LaunchTask启动任务消息，但是Executor为null。
   * 5.Executor收到Driver的KillTask消息，但是Executor为null。
   * 6.Executor和Driver失去连接。
   *
   */
  protected def exitExecutor(code: Int,
                             reason: String,
                             throwable: Throwable = null,
                             notifyDriver: Boolean = true) = {
    if (stopping.compareAndSet(false, true)) {
      val message = "Executor self-exiting due to : " + reason
      if (throwable != null) {
        logError(message, throwable)
      } else {
        logError(message)
      }

      if (notifyDriver && driver.nonEmpty) {
        driver.get.send(RemoveExecutor(executorId, new ExecutorLossReason(reason)))
      }
      self.send(Shutdown)
    } else {
      logInfo("Skip exiting executor since it's been already asked to exit before.")
    }
  }

  private def decommissionSelf(): Unit = {
    if (!env.conf.get(DECOMMISSION_ENABLED)) {
      logWarning(s"Receive decommission request, but decommission feature is disabled.")
      return
    } else if (decommissioned) {
      logWarning(s"Executor $executorId already started decommissioning.")
      return
    }
    val msg = s"Decommission executor $executorId."
    logInfo(msg)
    try {
      decommissioned = true
      if (env.conf.get(STORAGE_DECOMMISSION_ENABLED)) {
        env.blockManager.decommissionBlockManager()
      }
      if (executor != null) {
        executor.decommission()
      }
      // Shutdown the executor once all tasks are gone & any configured migrations completed.
      // Detecting migrations completion doesn't need to be perfect and we want to minimize the
      // overhead for executors that are not in decommissioning state as overall that will be
      // more of the executors. For example, this will not catch a block which is already in
      // the process of being put from a remote executor before migration starts. This trade-off
      // is viewed as acceptable to minimize introduction of any new locking structures in critical
      // code paths.

      val shutdownThread = new Thread("wait-for-blocks-to-migrate") {
        override def run(): Unit = {
          var lastTaskRunningTime = System.nanoTime()
          val sleep_time = 1000 // 1s
          // This config is internal and only used by unit tests to force an executor
          // to hang around for longer when decommissioned.
          val initialSleepMillis = env.conf.getInt(
            "spark.test.executor.decommission.initial.sleep.millis", sleep_time)
          if (initialSleepMillis > 0) {
            Thread.sleep(initialSleepMillis)
          }
          while (true) {
            logInfo("Checking to see if we can shutdown.")
            if (executor == null || executor.numRunningTasks == 0) {
              if (env.conf.get(STORAGE_DECOMMISSION_ENABLED)) {
                logInfo("No running tasks, checking migrations")
                val (migrationTime, allBlocksMigrated) = env.blockManager.lastMigrationInfo()
                // We can only trust allBlocksMigrated boolean value if there were no tasks running
                // since the start of computing it.
                if (allBlocksMigrated && (migrationTime > lastTaskRunningTime)) {
                  logInfo("No running tasks, all blocks migrated, stopping.")
                  exitExecutor(0, "Finished decommissioning", notifyDriver = true)
                } else {
                  logInfo("All blocks not yet migrated.")
                }
              } else {
                logInfo("No running tasks, no block migration configured, stopping.")
                exitExecutor(0, "Finished decommissioning", notifyDriver = true)
              }
            } else {
              logInfo("Blocked from shutdown by running ${executor.numRunningtasks} tasks")
              // If there is a running task it could store blocks, so make sure we wait for a
              // migration loop to complete after the last task is done.
              // Note: this is only advanced if there is a running task, if there
              // is no running task but the blocks are not done migrating this does not
              // move forward.
              lastTaskRunningTime = System.nanoTime()
            }
            Thread.sleep(sleep_time)
          }
        }
      }
      shutdownThread.setDaemon(true)
      shutdownThread.start()

      logInfo("Will exit when finished decommissioning")
    } catch {
      case e: Exception =>
        decommissioned = false
        logError("Unexpected error while decommissioning self", e)
    }
  }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging {

  // Message used internally to start the executor when the driver successfully accepted the
  // registration request.
  case object RegisteredExecutor

  case class Arguments(
      driverUrl: String,
      executorId: String,
      bindAddress: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String],
      userClassPath: mutable.ListBuffer[URL],
      resourcesFileOpt: Option[String],
      resourceProfileId: Int)

  def main(args: Array[String]): Unit = {
    val createFn: (RpcEnv, Arguments, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = { case (rpcEnv, arguments, env, resourceProfile) =>
      new CoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl, arguments.executorId,
        arguments.bindAddress, arguments.hostname, arguments.cores, arguments.userClassPath.toSeq,
        env, arguments.resourcesFileOpt, resourceProfile)
    }
    // 调用run方法
    run(parseArguments(args, this.getClass.getCanonicalName.stripSuffix("$")), createFn)
    System.exit(0)
  }

  def run(
      arguments: Arguments,
      backendCreateFn: (RpcEnv, Arguments, SparkEnv, ResourceProfile) =>
        CoarseGrainedExecutorBackend): Unit = {

    Utils.initDaemon(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(arguments.hostname)

      // Bootstrap to fetch the driver's Spark properties.
      // bootstrap取抓取driver节点的spark属性
      val executorConf = new SparkConf
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        arguments.bindAddress,
        arguments.hostname,
        -1,
        executorConf,
        new SecurityManager(executorConf),
        numUsableCores = 0,
        clientMode = true)

      var driver: RpcEndpointRef = null
      val nTries = 3
      for (i <- 0 until nTries if driver == null) {
        try {
          driver = fetcher.setupEndpointRefByURI(arguments.driverUrl)
        } catch {
          case e: Throwable => if (i == nTries - 1) {
            throw e
          }
        }
      }
      // CoarseGrainedExecutorBackend通过消息循环体向driver发送RetrieveSparkAppConfig消息，RetrieveSparkAppConfig是一个case object。
      // Driver端的CoarseGrainedSchedulerBackend消息循环体收到消息以后，将Spark的属性信息sparkProperties及加密key等内容封装成SparkAppConfig消息，
      // 将SparkAppConfig消息再回复给CoarseGrainedExecutorBackend。
      val cfg : SparkAppConfig = driver.askSync[SparkAppConfig](RetrieveSparkAppConfig(arguments.resourceProfileId))
      val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", arguments.appId))
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      // 从dirver节点获取属性信息，创建SparkEnv
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        // 这时SSL在独立模式下需要的
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }

      cfg.hadoopDelegationCreds.foreach { tokens =>
        SparkHadoopUtil.get.addDelegationTokens(tokens, driverConf)
      }

      driverConf.set(EXECUTOR_ID, arguments.executorId)
      val env = SparkEnv.createExecutorEnv(driverConf, arguments.executorId, arguments.bindAddress,
        arguments.hostname, arguments.cores, cfg.ioEncryptionKey, isLocal = false)

      val backend = backendCreateFn(env.rpcEnv, arguments, env, cfg.resourceProfile)
      env.rpcEnv.setupEndpoint("Executor", backend)
      arguments.workerUrl.foreach { url =>
        env.rpcEnv.setupEndpoint("WorkerWatcher",
          new WorkerWatcher(env.rpcEnv, url, isChildProcessStopping = backend.stopping))
      }
      env.rpcEnv.awaitTermination()
    }
  }

  def parseArguments(args: Array[String], classNameForEntry: String): Arguments = {
    var driverUrl: String = null
    var executorId: String = null
    var bindAddress: String = null
    var hostname: String = null
    var cores: Int = 0
    var resourcesFileOpt: Option[String] = None
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()
    var resourceProfileId: Int = DEFAULT_RESOURCE_PROFILE_ID

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--bind-address") :: value :: tail =>
          bindAddress = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--resourcesFile") :: value :: tail =>
          resourcesFileOpt = Some(value)
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          // worker url用于spark standalone模式，以加强与worker的分享
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case ("--resourceProfileId") :: value :: tail =>
          resourceProfileId = value.toInt
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit(classNameForEntry)
      }
    }

    if (hostname == null) {
      hostname = Utils.localHostName()
      log.info(s"Executor hostname is not provided, will use '$hostname' to advertise itself")
    }

    if (driverUrl == null || executorId == null || cores <= 0 || appId == null) {
      printUsageAndExit(classNameForEntry)
    }

    if (bindAddress == null) {
      bindAddress = hostname
    }

    Arguments(driverUrl, executorId, bindAddress, hostname, cores, appId, workerUrl,
      userClassPath, resourcesFileOpt, resourceProfileId)
  }

  private def printUsageAndExit(classNameForEntry: String): Unit = {
    // scalastyle:off println
    System.err.println(
      s"""
      |Usage: $classNameForEntry [options]
      |
      | Options are:
      |   --driver-url <driverUrl>
      |   --executor-id <executorId>
      |   --bind-address <bindAddress>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --resourcesFile <fileWithJSONResourceInformation>
      |   --app-id <appid>
      |   --worker-url <workerUrl>
      |   --user-class-path <url>
      |   --resourceProfileId <id>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }
}
