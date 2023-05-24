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

package org.apache.spark.scheduler.cluster

import java.util.Locale
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{StandaloneAppClient, StandaloneAppClientListener}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.resource.{ResourceProfile, ResourceUtils}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

/**
 * A [[SchedulerBackend]] implementation for Spark's standalone cluster manager.
 * 负责集群计算资源的管理和调度，这是从作业的角度来考虑的，注册给Master的时候，Master给我们分配资源，资源从Executor本身转过来向
 * StandaloneSchedulerBackend注册，这是从作业调度的角度来考虑的，不是从整个集群来考虑，整个集群是Master来管理计算资源的。
 */
private[spark] class StandaloneSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    masters: Array[String])
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with StandaloneAppClientListener
  with Logging {

  private[spark] var client: StandaloneAppClient = null
  private val stopping = new AtomicBoolean(false)
  private val launcherBackend = new LauncherBackend() {
    override protected def conf: SparkConf = sc.conf
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  @volatile var shutdownCallback: StandaloneSchedulerBackend => Unit = _
  @volatile private var appId: String = _

  private val registrationBarrier = new Semaphore(0)

  private val maxCores = conf.get(config.CORES_MAX)
  private val totalExpectedCores = maxCores.getOrElse(0)
  private val defaultProf = sc.resourceProfileManager.defaultResourceProfile

  /**
   * Master发指令给Worker去启动Executor所有的进程时加载的Main方法所在的入口类就是command中的CoarseGrainedExecutorBackend，
   * 在CoarseGrainedExecutorBackend中启动Executor（Executor是先注册，再实例化），Executor通过线程池并发执行Task，然后再调用它的run方法。
   */
  override def start(): Unit = {
    super.start()

    // SPARK-21159. The scheduler backend should only try to connect to the launcher when in client
    // mode. In cluster mode, the code that submits the application to the Master needs to connect
    // to the launcher instead.
    // 只有在client模式下scheduler backend 才去连接launcher
    // 在cluster 集群下，应用程序应提交给 Master
    if (sc.deployMode == "client") {
      launcherBackend.connect()
    }

    // The endpoint for executors to talk to us
    // executors节点与用户通信的端点
    val driverUrl = RpcEndpointAddress(
      sc.conf.get(config.DRIVER_HOST_ADDRESS),
      sc.conf.get(config.DRIVER_PORT),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")
    val extraJavaOpts = sc.conf.get(config.EXECUTOR_JAVA_OPTIONS)
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = sc.conf.get(config.EXECUTOR_CLASS_PATH)
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    val libraryPathEntries = sc.conf.get(config.EXECUTOR_LIBRARY_PATH)
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    // 测试时，将父类路径公开给子对象，由compute-classpath.{cmd.sh}计算路径
    // 当“*-provided”配置启用，子进程可使用所有需要的jar包
    val testingClassPath =
      if (sys.props.contains(IS_TESTING.key)) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }

    // Start executors with a few necessary configs for registering with the scheduler
    // 使用注册调度必要的一些配置启动executors
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    // 构建了一个command对象
    // 将command封装注册给Master，Master转过来要Worker启动具体的Executor。
    // command已经封装好指令，Executor具体要启动进程入口类CoarseGrainedExecutorBackend
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    val webUrl = sc.ui.map(_.webUrl).getOrElse("")
    val coresPerExecutor = conf.getOption(config.EXECUTOR_CORES.key).map(_.toInt)
    // If we're using dynamic allocation, set our initial executor limit to 0 for now.
    // ExecutorAllocationManager will send the real initial limit to the Master later.
    // 如果使用动态分配，现在将初始执行器限制设置为 0
    // ExecutorA1locationManager将实际的初始限制发送给 Master 节点
    val initialExecutorLimit =
      if (Utils.isDynamicAllocationEnabled(conf)) {
        Some(0)
      } else {
        None
      }
    val executorResourceReqs = ResourceUtils.parseResourceRequirements(conf,
      config.SPARK_EXECUTOR_PREFIX)
    val appDesc = ApplicationDescription(sc.appName, maxCores, sc.executorMemory, command,
      webUrl, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor, initialExecutorLimit,
      resourceReqsPerExecutor = executorResourceReqs)
    // 创建一个很重要的对象，然后调用它的client.start方法
    // 这个携带了command信息，command信息中指定了要启动的ExecutorBackend的实现类
    client = new StandaloneAppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
    client.start()
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    waitForRegistration()
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop(): Unit = {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def connected(appId: String): Unit = {
    logInfo("Connected to Spark cluster with app ID " + appId)
    this.appId = appId
    notifyContext()
    launcherBackend.setAppId(appId)
  }

  override def disconnected(): Unit = {
    notifyContext()
    if (!stopping.get) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String): Unit = {
    notifyContext()
    if (!stopping.get) {
      launcherBackend.setState(SparkAppHandle.State.KILLED)
      logError("Application has been killed. Reason: " + reason)
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        sc.stopInNewThread()
      }
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int): Unit = {
    logInfo("Granted executor ID %s on hostPort %s with %d core(s), %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(
      fullId: String,
      message: String,
      exitStatus: Option[Int],
      workerHost: Option[String]): Unit = {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code, exitCausedByApp = true, message)
      case None => ExecutorProcessLost(message, workerHost)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    removeExecutor(fullId.split("/")(1), reason)
  }

  override def executorDecommissioned(fullId: String,
      decommissionInfo: ExecutorDecommissionInfo): Unit = {
    logInfo(s"Asked to decommission executor $fullId")
    val execId = fullId.split("/")(1)
    decommissionExecutors(
      Array((execId, decommissionInfo)),
      adjustTargetNumExecutors = false,
      triggeredByExecutor = false)
    logInfo("Executor %s decommissioned: %s".format(fullId, decommissionInfo))
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logInfo("Worker %s removed: %s".format(workerId, message))
    removeWorker(workerId, host, message)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  protected override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
    // resources profiles not supported
    Option(client) match {
      case Some(c) =>
        val numExecs = resourceProfileToTotalExecs.getOrElse(defaultProf, 0)
        c.requestTotalExecutors(numExecs)
      case None =>
        logWarning("Attempted to request executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  /**
   * Kill the given list of executors through the Master.
   * @return whether the kill request is acknowledged.
   */
  protected override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    Option(client) match {
      case Some(c) => c.killExecutors(executorIds)
      case None =>
        logWarning("Attempted to kill executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  override def getDriverLogUrls: Option[Map[String, String]] = {
    val prefix = "SPARK_DRIVER_LOG_URL_"
    val driverLogUrls = sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase(Locale.ROOT), e._2)).toMap
    if (driverLogUrls.nonEmpty) Some(driverLogUrls) else None
  }

  private def waitForRegistration() = {
    registrationBarrier.acquire()
  }

  private def notifyContext() = {
    registrationBarrier.release()
  }

  private def stop(finalState: SparkAppHandle.State): Unit = {
    if (stopping.compareAndSet(false, true)) {
      try {
        super.stop()
        if (client != null) {
          client.stop()
        }
        val callback = shutdownCallback
        if (callback != null) {
          callback(this)
        }
      } finally {
        launcherBackend.setState(finalState)
        launcherBackend.close()
      }
    }
  }

}
