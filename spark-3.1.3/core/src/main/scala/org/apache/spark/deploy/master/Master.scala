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

package org.apache.spark.deploy.master

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription, ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.internal.config.UI._
import org.apache.spark.internal.config.Worker._
import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.resource.{ResourceRequirement, ResourceUtils}
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.util.{SparkUncaughtExceptionHandler, ThreadUtils, Utils}

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  // 继承的LeaderElectable设涉及Master的高可用新HA，
  // 继承的ThreadSafeRpcEndpoint使Maste人作为一个RpcEndpoint，实例化后首先会调用onStart方法
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

  // For application IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  private val workerTimeoutMs = conf.get(WORKER_TIMEOUT) * 1000
  private val retainedApplications = conf.get(RETAINED_APPLICATIONS)
  private val retainedDrivers = conf.get(RETAINED_DRIVERS)
  private val reaperIterations = conf.get(REAPER_ITERATIONS)
  // HA默认模式就是为None
  private val recoveryMode = conf.get(RECOVERY_MODE)
  // Executor挂掉时系统会尝试一定次数的重启（最多重启10次）
  private val maxExecutorRetries = conf.get(MAX_EXECUTOR_RETRIES)

  val workers = new HashSet[WorkerInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  private val waitingApps = new ArrayBuffer[ApplicationInfo]
  val apps = new HashSet[ApplicationInfo]

  private val idToWorker = new HashMap[String, WorkerInfo]
  private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]

  private val endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]
  private val addressToApp = new HashMap[RpcAddress, ApplicationInfo]
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private var nextAppNumber = 0

  private val drivers = new HashSet[DriverInfo]
  private val completedDrivers = new ArrayBuffer[DriverInfo]
  // Drivers currently spooled for scheduling
  private val waitingDrivers = new ArrayBuffer[DriverInfo]
  private var nextDriverNumber = 0

  Utils.checkHost(address.host)

  private val masterMetricsSystem =
    MetricsSystem.createMetricsSystem(MetricsSystemInstances.MASTER, conf, securityMgr)
  private val applicationMetricsSystem =
    MetricsSystem.createMetricsSystem(MetricsSystemInstances.APPLICATIONS, conf, securityMgr)
  private val masterSource = new MasterSource(this)

  // After onStart, webUi will be set
  private var webUi: MasterWebUI = null

  private val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else address.host
  }

  private val masterUrl = address.toSparkURL
  private var masterWebUiUrl: String = _

  private var state = RecoveryState.STANDBY

  private var persistenceEngine: PersistenceEngine = _

  private var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: ScheduledFuture[_] = _

  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  private val spreadOutApps = conf.get(SPREAD_OUT_APPS)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  // 默认maxCores时应用程序没有指定（通过Int.MaxValue）
  private val defaultCores = conf.get(DEFAULT_CORES)
  val reverseProxy = conf.get(UI_REVERSE_PROXY)
  if (defaultCores < 1) {
    throw new SparkException(s"${DEFAULT_CORES.key} must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  private val restServerEnabled = conf.get(MASTER_REST_SERVER_ENABLED)
  private var restServer: Option[StandaloneRestServer] = None
  private var restServerBoundPort: Option[Int] = None

  {
    val authKey = SecurityManager.SPARK_AUTH_SECRET_CONF
    require(conf.getOption(authKey).isEmpty || !restServerEnabled,
      s"The RestSubmissionServer does not support authentication via ${authKey}.  Either turn " +
        "off the RestSubmissionServer with spark.master.rest.enabled=false, or do not use " +
        "authentication.")
  }

  override def onStart(): Unit = {
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    // 构建一个Master的Web UI，查看向Master提交的应用册灰姑娘徐等信息
    webUi = new MasterWebUI(this, webUiPort)
    // 在WebUI的bind方法中启用了JettyServer
    webUi.bind()
    masterWebUiUrl = webUi.webUrl
    if (reverseProxy) {
      val uiReverseProxyUrl = conf.get(UI_REVERSE_PROXY_URL).map(_.stripSuffix("/"))
      if (uiReverseProxyUrl.nonEmpty) {
        System.setProperty("spark.ui.proxyBase", uiReverseProxyUrl.get)
        // If the master URL has a path component, it must end with a slash.
        // Otherwise the browser generates incorrect relative links
        masterWebUiUrl = uiReverseProxyUrl.get + "/"
      }
      webUi.addProxy()
      logInfo(s"Spark Master is acting as a reverse proxy. Master, Workers and " +
       s"Applications UIs are available at $masterWebUiUrl")
    }
    // 在一个守护线程中，启动调度机制，周期性检查Worker是否超时，当Worker节点超时后，会修改其状态或从Master中移除其相关的操作
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(
      () => Utils.tryLogNonFatalError { self.send(CheckForWorkerTimeOut) },
      0, workerTimeoutMs, TimeUnit.MILLISECONDS)
    // 默认情况下会启动Rest服务，可以通过该服务向Master提交各种请求
    if (restServerEnabled) {
      val port = conf.get(MASTER_REST_SERVER_PORT)
      // 其中调用new()函数创建一个StandaloneRestServer。StandaloneRestServer服务器响应请求提交的[RestSubmissionClient]，
      // 将被嵌入到standalone Master中，仅用于集群模式。服务器根据不同的情况使用不同的HTTP代码进行响应。
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    // BoundPortsResponse传入的参数restServerBoundPort是在Master的onStart方法中定义的
    // restServerBoundPort是通过restServer进行map操作启动赋值
    restServerBoundPort = restServer.map(_.start())
    // 度量相关操作，用于监控
    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    // 度量系统启动后，将主程序和应用程序度量handler处理程序附加到Web UI中
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    // Master HA相关的操作
    val serializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = recoveryMode match {
      /**
       * Spark生产环境下一般采用ZooKeeper作HA，且建议为3台Master，ZooKeeper会自动化管理Masters的切换。
       * 采用ZooKeeper作HA时，ZooKeeper会保存整个Spark集群运行时的元数据，
       * 包括Workers、Drivers、Applications、Executors。
       *
       * ZooKeeper遇到当前Active级别的Master出现故障时会从Standby Masters中选取出一台作为Active Master，
       * 但是要注意，被选举后到成为真正的Active Master之前需要从ZooKeeper中获取集群当前运行状态的元数据信息并进行恢复。
       *
       * 在Master切换的过程中，所有已经在运行的程序皆正常运行。因为Spark Application在运行前就已经通过Cluster Manager
       * 获得了计算资源，所以在运行时，Job本身的调度和处理和Master是没有任何关系的。
       *
       * 在Master的切换过程中唯一的影响是不能提交新的Job：一方面不能够提交新的应用程序给集群，因为只有Active Master
       * 才能接收新的程序提交请求；另一方面，已经运行的程序中也不能因为Action操作触发新的Job提交请求。
       */
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory = {
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        }
        // 创建一个Zookeeper-PersistenceEngline
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get(RECOVERY_MODE_FACTORY))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        // FILESYSTEM和NONE的方式采用MonarchyLeaderAgent的方式来完成Leader的选举，
        // 其实现是直接把传入的Master作为Leader
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def onStop(): Unit = {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader(): Unit = {
    self.send(ElectedLeader)
  }

  override def revokedLeadership(): Unit = {
    self.send(RevokedLeadership)
  }

  /**
   * master接收远处调用信息的方法的了啊
   * @return
   */
  override def receive: PartialFunction[Any, Unit] = {
    case ElectedLeader =>
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData(rpcEnv)
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(CompleteRecovery)
          }
        }, workerTimeoutMs, TimeUnit.MILLISECONDS)
      }

    case CompleteRecovery => completeRecovery()

    case RevokedLeadership =>
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)

    case WorkerDecommissioning(id, workerRef) =>
      if (state == RecoveryState.STANDBY) {
        workerRef.send(MasterInStandby)
      } else {
        // We use foreach since get gives us an option and we can skip the failures.
        idToWorker.get(id).foreach(decommissionWorker)
      }

    case DecommissionWorkers(ids) =>
      // The caller has already checked the state when handling DecommissionWorkersOnHosts,
      // so it should not be the STANDBY
      assert(state != RecoveryState.STANDBY)
      ids.foreach ( id =>
        // We use foreach since get gives us an option and we can skip the failures.
        idToWorker.get(id).foreach { w =>
          decommissionWorker(w)
          // Also send a message to the worker node to notify.
          w.endpoint.send(DecommissionWorker)
        }
      )

    // Worker是在启动后主动向Master注册的，所以如果在生产环境下加入新的Worker到正在运行的Spark集群上，
    // 此时不需要重新启动Spark集群就能够使用新加入的Worker，以提升处理能力。
    // 假如在生产环境中的集群中有500台机器，可能又新加入100台机器，
    // 这时不需要重新启动整个集群，就可以将100台新机器加入到集群
    // 。
    case RegisterWorker(
      id, workerHost, workerPort, workerRef, cores, memory, workerWebUiUrl,
      masterAddress, resources) =>
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      // 首先判断当前的Master是否是Standby的模式，如果是，就不处理
      if (state == RecoveryState.STANDBY) {
        workerRef.send(MasterInStandby)
        // idToWorker包含了所有已经注册的Worker的信息，然后会判断当前Master的内存数据结构idToWorker中是否已经有该Worker的注册，
        // 如果有，此时不会重复注册
      } else if (idToWorker.contains(id)) {
        workerRef.send(RegisteredWorker(self, masterWebUiUrl, masterAddress, true))
      } else {
        val workerResources = resources.map(r => r._1 -> WorkerResourceInfo(r._1, r._2.addresses))
        // WorkerInfo包括id、host、port、cores、memory、endpoint等内容
        // Master如果决定接收注册的Worker，首先会创建WorkerInfo对象来保存注册的Worker的信息
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          workerRef, workerWebUiUrl, workerResources)
        // 然后调用registerWorker执行具体的注册的过程
        if (registerWorker(worker)) {
          // zookeeper添加worker
          persistenceEngine.addWorker(worker)
          workerRef.send(RegisteredWorker(self, masterWebUiUrl, masterAddress, false))
          // schedule,调度
          schedule()
        } else {
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          workerRef.send(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }
    // Master收到RegisterApplication信息后便开始注册，注册后再次调用schedule()方法。
    //
    // Application提交给Master进行注册，Driver启动后会执行SparkContext的初始化，进而导致StandaloneSchedulerBackend的产生，
    // 其内部有StandaloneAppClient。StandaloneAppClient内部有ClientEndpoint。
    // ClientEndpoint来发送RegisterApplication信息给Master。
    case RegisterApplication(description, driver) =>
      // TODO Prevent repeated registrations from some driver
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        // 创建ApplicationInfo
        val app = createApplication(description, driver)
        // 完成application的注册
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        // 通过持久化引擎（如ZooKeeper）把注册信息持久化
        persistenceEngine.addApplication(app)
        // 给driver发送RegisteredApplication的信息
        driver.send(RegisteredApplication(app.id, self))
        schedule()
      }

    /**
     * 接收worker传进来的ExecutorStateChanged，比如executor退出的
     */
    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) =>
          val appInfo = idToApp(appId)
          val oldState = exec.state
          exec.state = state

          if (state == ExecutorState.RUNNING) {
            assert(oldState == ExecutorState.LAUNCHING,
              s"executor $execId state transfer from $oldState to RUNNING is illegal")
            appInfo.resetRetryCount()
          }

          exec.application.driver.send(ExecutorUpdated(execId, state, message, exitStatus, None))

          if (ExecutorState.isFinished(state)) {
            // Remove this executor from the worker and app
            // 从worker和app中删掉executor
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            // If an application has already finished, preserve its
            // state to display its information properly on the UI
            // 如果应用程序已经完成，保存应用程序状态，以在 UI页面上正确显示信息
            if (!appInfo.isFinished) {
              appInfo.removeExecutor(exec)
            }
            exec.worker.removeExecutor(exec)

            val normalExit = exitStatus == Some(0)
            // Only retry certain number of times so we don't go into an infinite loop.
            // Important note: this code path is not exercised by tests, so be very careful when
            // changing this `if` condition.
            // We also don't count failures from decommissioned workers since they are "expected."
            // 只重试一定次数，这样就不会进入无限循环
            if (!normalExit
                && oldState != ExecutorState.DECOMMISSIONED
                && appInfo.incrementRetryCount() >= maxExecutorRetries
                && maxExecutorRetries >= 0) { // < 0 disables this application-killing path
              val execs = appInfo.executors.values
              if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                  s"${appInfo.retryCount} times; removing it")
                removeApplication(appInfo, ApplicationState.FAILED)
              }
            }
          }
          schedule()
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }

    case DriverStateChanged(driverId, state, exception) =>

      // 如果Driver的各个状态符合ERROR FINISHED KILLED FAILED就将其清理掉
      state match {
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          // removeDriver后，再次调用schedule方法
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }

    case Heartbeat(workerId, worker) =>
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            worker.send(ReconnectWorker(masterUrl))
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }

    case MasterChangeAcknowledged(appId) =>
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }

    case WorkerSchedulerStateResponse(workerId, execResponses, driverResponses) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = execResponses.filter(
            exec => idToApp.get(exec.desc.appId).isDefined)
          for (exec <- validExecutors) {
            val (execDesc, execResources) = (exec.desc, exec.resources)
            val app = idToApp(execDesc.appId)
            val execInfo = app.addExecutor(
              worker, execDesc.cores, execResources, Some(execDesc.execId))
            worker.addExecutor(execInfo)
            worker.recoverResources(execResources)
            execInfo.copyState(execDesc)
          }

          for (driver <- driverResponses) {
            val (driverId, driverResource) = (driver.driverId, driver.resources)
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              driver.withResources(driverResource)
              worker.recoverResources(driverResource)
              worker.addDriver(driver)
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }
      // 如果WorkerState状态为UNKNOWN（Worker不响应），就把它删除，如果以集群方式运行，
      // driver失败后可以重新启动，最后把状态变回ALIVE。注意，这里要加入--supervise这个参数
      if (canCompleteRecovery) { completeRecovery() }

    case WorkerLatestState(workerId, executors, driverIds) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          for (exec <- executors) {
            val executorMatches = worker.executors.exists {
              case (_, e) => e.application.id == exec.appId && e.id == exec.execId
            }
            if (!executorMatches) {
              // master doesn't recognize this executor. So just tell worker to kill it.
              worker.endpoint.send(KillExecutor(masterUrl, exec.appId, exec.execId))
            }
          }

          for (driverId <- driverIds) {
            val driverMatches = worker.drivers.exists { case (id, _) => id == driverId }
            if (!driverMatches) {
              // master doesn't recognize this driver. So just tell worker to kill it.
              worker.endpoint.send(KillDriver(driverId))
            }
          }
        case None =>
          logWarning("Worker state from unknown worker: " + workerId)
      }

    case UnregisterApplication(applicationId) =>
      logInfo(s"Received unregister request from application $applicationId")
      idToApp.get(applicationId).foreach(finishApplication)

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // Master收到Driver ClientEndpoint的RequestSubmitDriver消息以后，
    // 就将Driver的信息加入到waitingDrivers和drivers的数据结构中。
    // 然后进行schedule()资源分配，Master向Worker发送LaunchDriver的消息指令。
    case RequestSubmitDriver(description) =>
      // 若 state 不为 ALIVE，直接向 Client 返回 SubmitDriverResponse(selffalse
      // None_msg)消息
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
        context.reply(SubmitDriverResponse(self, false, None, msg))
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        // 使用description创建driver，该方法返回DriverDescription
        val driver = createDriver(description)
        // watingDrviers等待在调度数组中加入该driver
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        // 用schedule方法调度资源
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        context.reply(SubmitDriverResponse(self, true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}"))
      }

    case RequestKillDriver(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillDriverResponse(self, driverId, success = false, msg))
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self.send(DriverStateChanged(driverId, DriverState.KILLED, None))
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.endpoint.send(KillDriver(driverId))
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            context.reply(KillDriverResponse(self, driverId, success = true, msg))
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            context.reply(KillDriverResponse(self, driverId, success = false, msg))
        }
      }

    case RequestDriverStatus(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver status in ALIVE state."
        context.reply(
          DriverStatusResponse(found = false, None, None, None, Some(new Exception(msg))))
      } else {
        (drivers ++ completedDrivers).find(_.id == driverId) match {
          case Some(driver) =>
            context.reply(DriverStatusResponse(found = true, Some(driver.state),
              driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception))
          case None =>
            context.reply(DriverStatusResponse(found = false, None, None, None, None))
        }
      }

    case RequestMasterState =>
      context.reply(MasterStateResponse(
        address.host, address.port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state))

    case BoundPortsRequest =>
      context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))

    case RequestExecutors(appId, requestedTotal) =>
      context.reply(handleRequestExecutors(appId, requestedTotal))

    case KillExecutors(appId, executorIds) =>
      val formattedExecutorIds = formatExecutorIds(executorIds)
      context.reply(handleKillExecutors(appId, formattedExecutorIds))

    case DecommissionWorkersOnHosts(hostnames) =>
      if (state != RecoveryState.STANDBY) {
        context.reply(decommissionWorkersOnHosts(hostnames))
      } else {
        context.reply(0)
      }
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"$address got disassociated, removing it.")
    addressToWorker.get(address).foreach(removeWorker(_, s"${address} got disassociated"))
    addressToApp.get(address).foreach(finishApplication)
    if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }

  private def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  private def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]): Unit = {
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        registerApplication(app)
        app.state = ApplicationState.UNKNOWN
        app.driver.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.endpoint.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  private def completeRecovery(): Unit = {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    // 使用短同步周期确保“only-once”恢复一次语义
    if (state != RecoveryState.RECOVERING) { return }
    state = RecoveryState.COMPLETING_RECOVERY

    // Kill off any workers and apps that didn't respond to us.
    // 杀掉不响应消息的 workers和apps
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(
      removeWorker(_, "Not responding for recovery"))

    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Update the state of recovered apps to RUNNING
    //  // 将恢复的应用程序的状态更新为正在运行
    apps.filter(_.state == ApplicationState.WAITING).foreach(_.state = ApplicationState.RUNNING)

    // Reschedule drivers which were not claimed by any workers
    // 重新调度drivers，其未被任何 workers声明
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d)
      } else {
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    state = RecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
   * Schedule executors to be launched on the workers.
   * Returns an array containing number of cores assigned to each worker.
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor per application may be launched on each
   * worker during one single schedule iteration.
   * Note that when `spark.executor.cores` is not set, we may still launch multiple executors from
   * the same application on the same worker. Consider appA and appB both have one executor running
   * on worker1, and appA.coresLeft > 0, then appB is finished and release all its cores on worker1,
   * thus for the next schedule iteration, appA launches a new executor that grabs all the free
   * cores on worker1, therefore we get multiple executors from appA running on worker1.
   *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   */
  private def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    val coresPerExecutor = app.desc.coresPerExecutor
    // 表示每个Executor最小分配的core个数
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    val resourceReqsPerExecutor = app.desc.resourceReqsPerExecutor
    val numUsable = usableWorkers.length
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    // 用于计算app.coresLeft与可用的Worker中可用的Cores的和的最小值
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    def canLaunchExecutorForApp(pos: Int): Boolean = {
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor
      val assignedExecutorNum = assignedExecutors(pos)

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutorNum == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutorNum * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        val assignedResources = resourceReqsPerExecutor.map {
          req => req.resourceName -> req.amount * assignedExecutorNum
        }.toMap
        val resourcesFree = usableWorkers(pos).resourcesAmountFree.map {
          case (rName, free) => rName -> (free - assignedResources.getOrElse(rName, 0))
        }
        val enoughResources = ResourceUtils.resourcesMeetRequirements(
          resourcesFree, resourceReqsPerExecutor)
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && enoughResources && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutorForApp)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutorForApp(pos)) {
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          // 如果每个Worker下面只能为当前的应用程序分配一个Executor，那么每次只分配一个Core
          // 如果每个worker上启动一个Executor，那么每次迭代在Executor 上分配1
          // 个核，否则，每次迭代都将把内核分配给一个新的 Executor
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          // 展开应用程序意味着将Executors 展开到尽可能多的workers 节点。如果不展
          // 开，将对这个 workers的Executors 进行调度，直到使用它的全部资源。否则
          // 只是移动到下一个 worker节点
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutorForApp)
    }
    assignedCores
  }

  /**
   * Schedule and launch executors on workers
   */
  private def startExecutorsOnWorkers(): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    // 这是一个非常简单的 FIFO 调度。我们尝试在队列中推入第一个应用程序，然后推入第2个应用程序等
    for (app <- waitingApps) {
      // 筛选出workers，其没有足够资源来启动Executor
      val coresPerExecutor = app.desc.coresPerExecutor.getOrElse(1)
      // If the cores left is less than the coresPerExecutor,the cores left will not be allocated
      if (app.coresLeft >= coresPerExecutor) {
        // Filter out workers that don't have enough resources to launch an executor
        // 如果剩余的核心小于coresPerExecutor，则不会分配剩余的核心
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(canLaunchExecutor(_, app.desc))
            // 在此基础上进行排序，产生计算资源由大到小的coresFree数据结构
          .sortBy(_.coresFree).reverse
        val appMayHang = waitingApps.length == 1 &&
          waitingApps.head.executors.isEmpty && usableWorkers.isEmpty
        if (appMayHang) {
          logWarning(s"App ${app.id} requires more resource than any of Workers could have.")
        }
        // spreadOutApps默认让应用程序尽可能地多运行在所有的Node上
        val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)

        // 现在我们决定每个worker分配多少个cores，进行分配
        // Now that we've decided how many cores to allocate on each worker, let's allocate them
        for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
          allocateWorkerResourceToExecutors(
            app, assignedCores(pos), app.desc.coresPerExecutor, usableWorkers(pos))
        }
      }
    }
  }

  /**
   * Allocate a worker's resources to one or more executors.
   * @param app the info of the application which the executors belong to
   * @param assignedCores number of cores on this worker for this application
   * @param coresPerExecutor number of cores per executor
   * @param worker the worker info
   */
  private def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit = {
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
    // 如果指定了每个 Executor 的内核数，我们就将分配的内核无剩余地均分给 worker 节点的
    // Executors。否则，我们启动一个单一的 Executor，抓住这个 worker 节点所有的assignedCores
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
    for (i <- 1 to numExecutors) {
      val allocated = worker.acquireResources(app.desc.resourceReqsPerExecutor)
      // 增加一个executor，记录Executor的相关信息
      val exec = app.addExecutor(worker, coresToAssign, allocated)
      // standaloneschedulerbackend中的standaloneappclient向master发送registterapplication注册请求
      // master受理后通过launchExecutor方法在worker节点启动一个executorRunner对象，该对象由于管理一个Executor进程
      launchExecutor(worker, exec)
      app.state = ApplicationState.RUNNING
    }
  }

  private def canLaunch(
      worker: WorkerInfo,
      memoryReq: Int,
      coresReq: Int,
      resourceRequirements: Seq[ResourceRequirement])
    : Boolean = {
    val enoughMem = worker.memoryFree >= memoryReq
    val enoughCores = worker.coresFree >= coresReq
    val enoughResources = ResourceUtils.resourcesMeetRequirements(
      worker.resourcesAmountFree, resourceRequirements)
    enoughMem && enoughCores && enoughResources
  }

  /**
   * @return whether the worker could launch the driver represented by DriverDescription
   */
  private def canLaunchDriver(worker: WorkerInfo, desc: DriverDescription): Boolean = {
    canLaunch(worker, desc.mem, desc.cores, desc.resourceReqs)
  }

  /**
   * @return whether the worker could launch the executor according to application's requirement
   */
  private def canLaunchExecutor(worker: WorkerInfo, desc: ApplicationDescription): Boolean = {
    canLaunch(
      worker,
      desc.memoryPerExecutorMB,
      desc.coresPerExecutor.getOrElse(1),
      desc.resourceReqsPerExecutor)
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   *
   * Schedule调用的时机：每次有新的应用程序提交或者集群资源状况发生改变的时候（包括Executor增加或者减少、Worker增加或者减少等）。
   */
  private def schedule(): Unit = {
    if (state != RecoveryState.ALIVE) {
      return
    }
    // Drivers take strict precedence over executors
    // drivers优于executors
    // RecoveryState若不为ALIVE，则直接返回，否则使用Random.shuffle将Workers集合打乱，
    // 过滤出ALIVE的Worker，生成新的集合shuffledAliveWorkers，尽量考虑到选择Driver的负载均衡
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    // 在for语句中遍历waitingDrivers队列，判断Worker剩余内存和剩余物理核是否满足Driver需求，如满足
    // ，则调用launchDriver(worker,driver)方法在选中的Worker上启动Driver进程。
    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      // 遍历waitingDrivers 以循环的方式给每个等候的driver分配Worker。对于每个driver，我们从分配
      // 给driver的最后一个Worker开始，继续前进，直到所有活跃的 Worker 节点
      var launched = false
      var isClusterIdle = true
      var numWorkersVisited = 0
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        isClusterIdle = worker.drivers.isEmpty && worker.executors.isEmpty
        numWorkersVisited += 1
        if (canLaunchDriver(worker, driver.desc)) {
          val allocated = worker.acquireResources(driver.desc.resourceReqs)
          driver.withResources(allocated)
          // 如果worker剩余内存和剩余物理河满足Driver需求，如满足，则调用launchDriver方法在选中的worker上启动Driver进程
          launchDriver(worker, driver)
          waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
      if (!launched && isClusterIdle) {
        logWarning(s"Driver ${driver.id} requires more resource than any of Workers could have.")
      }
    }
    // 调用startExecutorsOnWorkers()为当前的程序调度和启动Worker的Executor，默认情况下排队的方式是FIFO
    startExecutorsOnWorkers()
  }

  private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec)
    // Master要通过远程通信发指令给Worker来具体启动ExecutorBackend进程
    worker.endpoint.send(LaunchExecutor(masterUrl, exec.application.id, exec.id,
      exec.application.desc, exec.cores, exec.memory, exec.resources))
    // 向Driver发回ExecutorAdded消息，消息携带 worker的id号、worker的host和
    // port、分配的核的个数和内存大小
    exec.application.driver.send(
      ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
  }

  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    // 如果Worker的状态是DEAD的状态，则直接过滤掉
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        // 对于UNKNOWN的内容，调用removeWorker进行清理（包括清理该Worker下的Executors和Drivers）。
        // 其中，UNKNOWN的情况：Master进行切换时，先对Worker发UNKNOWN消息，
        // 只有当Master收到Worker正确的回复消息，才将状态标识为正常、
        // 。
        removeWorker(oldWorker, "Worker replaced by a new worker with same address")
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  /**
   * Decommission all workers that are active on any of the given hostnames. The decommissioning is
   * asynchronously done by enqueueing WorkerDecommission messages to self. No checks are done about
   * the prior state of the worker. So an already decommissioned worker will match as well.
   *
   * @param hostnames: A list of hostnames without the ports. Like "localhost", "foo.bar.com" etc
   *
   * Returns the number of workers that matched the hostnames.
   */
  private def decommissionWorkersOnHosts(hostnames: Seq[String]): Integer = {
    val hostnamesSet = hostnames.map(_.toLowerCase(Locale.ROOT)).toSet
    val workersToRemove = addressToWorker
      .filterKeys(addr => hostnamesSet.contains(addr.host.toLowerCase(Locale.ROOT)))
      .values

    val workersToRemoveHostPorts = workersToRemove.map(_.hostPort)
    logInfo(s"Decommissioning the workers with host:ports ${workersToRemoveHostPorts}")

    // The workers are removed async to avoid blocking the receive loop for the entire batch
    self.send(DecommissionWorkers(workersToRemove.map(_.id).toSeq))

    // Return the count of workers actually removed
    workersToRemove.size
  }

  private def decommissionWorker(worker: WorkerInfo): Unit = {
    if (worker.state != WorkerState.DECOMMISSIONED) {
      logInfo("Decommissioning worker %s on %s:%d".format(worker.id, worker.host, worker.port))
      worker.setState(WorkerState.DECOMMISSIONED)
      for (exec <- worker.executors.values) {
        logInfo("Telling app of decommission executors")
        exec.application.driver.send(ExecutorUpdated(
          exec.id, ExecutorState.DECOMMISSIONED,
          Some("worker decommissioned"), None,
          // worker host is being set here to let the driver know that the host (aka. worker)
          // is also being decommissioned. So the driver can unregister all the shuffle map
          // statues located at this host when it receives the executor lost event.
          Some(worker.host)))
        exec.state = ExecutorState.DECOMMISSIONED
        exec.application.removeExecutor(exec)
      }
      // On recovery do not add a decommissioned executor
      persistenceEngine.removeWorker(worker)
    } else {
      logWarning("Skipping decommissioning worker %s on %s:%d as worker is already decommissioned".
        format(worker.id, worker.host, worker.port))
    }
  }

  private def removeWorker(worker: WorkerInfo, msg: String): Unit = {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address

    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      exec.application.driver.send(ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None, Some(worker.host)))
      exec.state = ExecutorState.LOST
      exec.application.removeExecutor(exec)
    }
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    logInfo(s"Telling app of lost worker: " + worker.id)
    apps.filterNot(completedApps.contains(_)).foreach { app =>
      app.driver.send(WorkerRemoved(worker.id, worker.host, msg))
    }
    persistenceEngine.removeWorker(worker)
    schedule()
  }

  private def relaunchDriver(driver: DriverInfo): Unit = {
    // We must setup a new driver with a new driver id here, because the original driver may
    // be still running. Consider this scenario: a worker is network partitioned with master,
    // the master then relaunches driver driverID1 with a driver id driverID2, then the worker
    // reconnects to master. From this point on, if driverID2 is equal to driverID1, then master
    // can not distinguish the statusUpdate of the original driver and the newly relaunched one,
    // for example, when DriverStateChanged(driverID1, KILLED) arrives at master, master will
    // remove driverID1, so the newly relaunched driver disappears too. See SPARK-19900 for details.
    removeDriver(driver.id, DriverState.RELAUNCHING, None)
    val newDriver = createDriver(driver.desc)
    persistenceEngine.addDriver(newDriver)
    drivers.add(newDriver)
    waitingDrivers += newDriver

    schedule()
  }

  private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
      ApplicationInfo = {
    // applicationinfo创建时间
    val now = System.currentTimeMillis()
    val date = new Date(now)
    // 由date生成application id
    val appId = newApplicationId(date)
    // 使用desc、date、driver、defaultCores等作为参数构造一个ApplicatiOnInfo对象并返回
    new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
  }

  private def registerApplication(app: ApplicationInfo): Unit = {
    // driver的地址，用于Master和Driver通信
    val appAddress = app.driver.address
    //  如果addressToApp中已经有了该Driver地址，说明该Driver已经注册过了 ，直接return
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }
    // 向度量系统注册
    applicationMetricsSystem.registerSource(app.appSource)
    // apps是一个HashSet，保存数据不能重复，向HashSet加入app
    apps += app
    // idToApp是一个HashMap，该HashMap用于保存id和app 的对应关系
    idToApp(app.id) = app
    // endpointToApp是一个HashMap，该HashMap 用于保存driver和app的对应关系
    endpointToApp(app.driver) = app
    // addressToApp 是一个HashMap，记录app Driver的地址和app 的对应关系
    addressToApp(appAddress) = app
    // waitingApps是一个数组，记录等待调度的app记录
    waitingApps += app
  }

  private def finishApplication(app: ApplicationInfo): Unit = {
    removeApplication(app, ApplicationState.FINISHED)
  }

  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value): Unit = {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      endpointToApp -= app.driver
      addressToApp -= app.driver.address

      if (completedApps.size >= retainedApplications) {
        val toRemove = math.max(retainedApplications / 10, 1)
        completedApps.take(toRemove).foreach { a =>
          applicationMetricsSystem.removeSource(a.appSource)
        }
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      for (exec <- app.executors.values) {
        killExecutor(exec)
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver.send(ApplicationRemoved(state.toString))
      }
      persistenceEngine.removeApplication(app)
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      workers.foreach { w =>
        w.endpoint.send(ApplicationFinished(app.id))
      }
    }
  }

  /**
   * Handle a request to set the target number of executors for this application.
   *
   * If the executor limit is adjusted upwards, new executors will be launched provided
   * that there are workers with sufficient resources. If it is adjusted downwards, however,
   * we do not kill existing executors until we explicitly receive a kill request.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleRequestExecutors(appId: String, requestedTotal: Int): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requested to set total executors to $requestedTotal.")
        appInfo.executorLimit = requestedTotal
        schedule()
        true
      case None =>
        logWarning(s"Unknown application $appId requested $requestedTotal total executors.")
        false
    }
  }

  /**
   * Handle a kill request from the given application.
   *
   * This method assumes the executor limit has already been adjusted downwards through
   * a separate [[RequestExecutors]] message, such that we do not launch new executors
   * immediately after the old ones are removed.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleKillExecutors(appId: String, executorIds: Seq[Int]): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requests to kill executors: " + executorIds.mkString(", "))
        val (known, unknown) = executorIds.partition(appInfo.executors.contains)
        known.foreach { executorId =>
          val desc = appInfo.executors(executorId)
          appInfo.removeExecutor(desc)
          killExecutor(desc)
        }
        if (unknown.nonEmpty) {
          logWarning(s"Application $appId attempted to kill non-existent executors: "
            + unknown.mkString(", "))
        }
        schedule()
        true
      case None =>
        logWarning(s"Unregistered application $appId requested us to kill executors!")
        false
    }
  }

  /**
   * Cast the given executor IDs to integers and filter out the ones that fail.
   *
   * All executors IDs should be integers since we launched these executors. However,
   * the kill interface on the driver side accepts arbitrary strings, so we need to
   * handle non-integer executor IDs just to be safe.
   */
  private def formatExecutorIds(executorIds: Seq[String]): Seq[Int] = {
    executorIds.flatMap { executorId =>
      try {
        Some(executorId.toInt)
      } catch {
        case e: NumberFormatException =>
          logError(s"Encountered executor with a non-integer ID: $executorId. Ignoring")
          None
      }
    }
  }

  /**
   * Ask the worker on which the specified executor is launched to kill the executor.
   */
  private def killExecutor(exec: ExecutorDesc): Unit = {
    exec.worker.removeExecutor(exec)
    exec.worker.endpoint.send(KillExecutor(masterUrl, exec.application.id, exec.id))
    exec.state = ExecutorState.KILLED
  }

  /** Generate a new app ID given an app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  private def timeOutDeadWorkers(): Unit = {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - workerTimeoutMs).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        val workerTimeoutSecs = TimeUnit.MILLISECONDS.toSeconds(workerTimeoutMs)
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, workerTimeoutSecs))
        removeWorker(worker, s"Not receiving heartbeat for $workerTimeoutSecs seconds")
      } else {
        if (worker.lastHeartbeat < currentTime - ((reaperIterations + 1) * workerTimeoutMs)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  private def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  private def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  private def launchDriver(worker: WorkerInfo, driver: DriverInfo): Unit = {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    worker.addDriver(driver)
    driver.worker = Some(worker)
    // 发指令给worker，让远程的worker启动driver，driver启动之后，Driver的状态就变成DriverState.RUNNING
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc, driver.resources))
    driver.state = DriverState.RUNNING
  }

  private def removeDriver(
      driverId: String,
      finalState: DriverState,
      exception: Option[Exception]): Unit = {
    drivers.find(d => d.id == driverId) match {
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        drivers -= driver
        if (completedDrivers.size >= retainedDrivers) {
          val toRemove = math.max(retainedDrivers / 10, 1)
          completedDrivers.trimStart(toRemove)
        }
        completedDrivers += driver
        persistenceEngine.removeDriver(driver)
        driver.state = finalState
        driver.exception = exception
        driver.worker.foreach(w => w.removeDriver(driver))
        // 清掉相关数据以后，再次调用schedule方法
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}

private[deploy] object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  def main(argStrings: Array[String]): Unit = {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)

    val conf = new SparkConf
    // 构建参数解析的实例
    val args = new MasterArguments(argStrings, conf)
    // 启动RPC通信环境以及Mater的ROC通信终端
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    // 构建PRC通信环境
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    // 构建PRC通信终端，实例化Mater
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    // 向Master的通信终端发送请求，获取绑定的端口号
    // 包含Mas|ter的Web UI监听端口号和REST的监听端口号

    // 通过masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)给Master自己发送一个消息BoundPortsRequest，
    // 是一个case object。发送消息BoundPortsRequest给自己，确保masterEndpoint正常启动起来。
    // 返回消息的类型是BoundPortsResponse， 是一个case class
    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}
