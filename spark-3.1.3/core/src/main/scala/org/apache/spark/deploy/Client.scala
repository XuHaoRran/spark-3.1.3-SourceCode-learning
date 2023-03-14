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

package org.apache.spark.deploy

import java.util.concurrent.TimeUnit

import scala.collection.mutable.HashSet
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

import org.apache.log4j.Logger

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.Network.RPC_ASK_TIMEOUT
import org.apache.spark.resource.ResourceUtils
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.{SparkExitCode, ThreadUtils, Utils}

/**
 * Proxy that relays messages to the driver.
 *
 * We currently don't support retry if submission fails. In HA mode, client will submit request to
 * all masters and see which one could handle it.
 */
private class ClientEndpoint(
    override val rpcEnv: RpcEnv,
    driverArgs: ClientArguments,
    masterEndpoints: Seq[RpcEndpointRef],
    conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging {

  // A scheduled executor used to send messages at the specified time.
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("client-forward-message")
  // Used to provide the implicit parameter of `Future` methods.
  private val forwardMessageExecutionContext =
    ExecutionContext.fromExecutor(forwardMessageThread,
      t => t match {
        case ie: InterruptedException => // Exit normally
        case e: Throwable =>
          logError(e.getMessage, e)
          System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
      })

   private val lostMasters = new HashSet[RpcAddress]
   private var activeMasterEndpoint: RpcEndpointRef = null
   private val waitAppCompletion = conf.get(config.STANDALONE_SUBMIT_WAIT_APP_COMPLETION)
   private val REPORT_DRIVER_STATUS_INTERVAL = 10000
   private var submittedDriverID = ""
   private var driverStatusReported = false


  private def getProperty(key: String, conf: SparkConf): Option[String] = {
    sys.props.get(key).orElse(conf.getOption(key))
  }

  /**
   * onStart方法中向Master提交注册。
   * 这里通过masterEndpoint向Master发送RequestSubmitDriver(driverDescription)请求，完成Driver的注册。
   */
  override def onStart(): Unit = {
    driverArgs.cmd match {
      case "launch" =>
        // TODO: We could add an env variable here and intercept it in `sc.addJar` that would
        //       truncate filesystem paths similar to what YARN does. For now, we just require
        //       people call `addJar` assuming the jar is in the same directory.
        val mainClass = "org.apache.spark.deploy.worker.DriverWrapper"

        val classPathConf = config.DRIVER_CLASS_PATH.key
        val classPathEntries = getProperty(classPathConf, conf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val libraryPathConf = config.DRIVER_LIBRARY_PATH.key
        val libraryPathEntries = getProperty(libraryPathConf, conf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val extraJavaOptsConf = config.DRIVER_JAVA_OPTIONS.key
        val extraJavaOpts = getProperty(extraJavaOptsConf, conf)
          .map(Utils.splitCommandString).getOrElse(Seq.empty)

        val sparkJavaOpts = Utils.sparkJavaOpts(conf)
        val javaOpts = sparkJavaOpts ++ extraJavaOpts
        val command = new Command(mainClass,
          Seq("{{WORKER_URL}}", "{{USER_JAR}}", driverArgs.mainClass) ++ driverArgs.driverOptions,
          sys.env, classPathEntries, libraryPathEntries, javaOpts)
        val driverResourceReqs = ResourceUtils.parseResourceRequirements(conf,
          config.SPARK_DRIVER_PREFIX)
        val driverDescription = new DriverDescription(
          driverArgs.jarUrl,
          driverArgs.memory,
          driverArgs.cores,
          driverArgs.supervise,
          command,
          driverResourceReqs)
        asyncSendToMasterAndForwardReply[SubmitDriverResponse](
          // 这里通过masterEndpoint向Master发送RequestSubmitDriver(driverDescription)请求，完成Driver的注册。
          RequestSubmitDriver(driverDescription))

      case "kill" =>
        val driverId = driverArgs.driverId
        submittedDriverID = driverId
        asyncSendToMasterAndForwardReply[KillDriverResponse](RequestKillDriver(driverId))
    }
    logInfo("... waiting before polling master for driver state")
    forwardMessageThread.scheduleAtFixedRate(() => Utils.tryLogNonFatalError {
      monitorDriverStatus()
    }, 5000, REPORT_DRIVER_STATUS_INTERVAL, TimeUnit.MILLISECONDS)
  }

  /**
   * Send the message to master and forward the reply to self asynchronously.
   */
  private def asyncSendToMasterAndForwardReply[T: ClassTag](message: Any): Unit = {
    for (masterEndpoint <- masterEndpoints) {
      masterEndpoint.ask[T](message).onComplete {
        case Success(v) => self.send(v)
        case Failure(e) =>
          logWarning(s"Error sending messages to master $masterEndpoint", e)
      }(forwardMessageExecutionContext)
    }
  }

  private def monitorDriverStatus(): Unit = {
    if (submittedDriverID != "") {
      asyncSendToMasterAndForwardReply[DriverStatusResponse](RequestDriverStatus(submittedDriverID))
    }
  }

  /**
   * Processes and reports the driver status then exit the JVM if the
   * waitAppCompletion is set to false, else reports the driver status
   * if debug logs are enabled.
   */

  def reportDriverStatus(
      found: Boolean,
      state: Option[DriverState],
      workerId: Option[String],
      workerHostPort: Option[String],
      exception: Option[Exception]): Unit = {
    if (found) {
      // Using driverStatusReported to avoid writing following
      // logs again when waitAppCompletion is set to true
      if (!driverStatusReported) {
        driverStatusReported = true
        logInfo(s"State of $submittedDriverID is ${state.get}")
        // Worker node, if present
        (workerId, workerHostPort, state) match {
          case (Some(id), Some(hostPort), Some(DriverState.RUNNING)) =>
            logInfo(s"Driver running on $hostPort ($id)")
          case _ =>
        }
      }
      // Exception, if present
      exception match {
        case Some(e) =>
          logError(s"Exception from cluster was: $e")
          e.printStackTrace()
          System.exit(-1)
        case _ =>
          state.get match {
            case DriverState.FINISHED | DriverState.FAILED |
                 DriverState.ERROR | DriverState.KILLED =>
              logInfo(s"State of driver $submittedDriverID is ${state.get}, " +
                s"exiting spark-submit JVM.")
              System.exit(0)
            case _ =>
              if (!waitAppCompletion) {
                logInfo(s"spark-submit not configured to wait for completion, " +
                  s"exiting spark-submit JVM.")
                System.exit(0)
              } else {
                logDebug(s"State of driver $submittedDriverID is ${state.get}, " +
                  s"continue monitoring driver status.")
              }
          }
      }
    } else if (exception.exists(e => Utils.responseFromBackup(e.getMessage))) {
       logDebug(s"The status response is reported from a backup spark instance. So, ignored.")
    } else {
        logError(s"ERROR: Cluster master did not recognize $submittedDriverID")
        System.exit(-1)
    }
  }
  override def receive: PartialFunction[Any, Unit] = {

    case SubmitDriverResponse(master, success, driverId, message) =>
      logInfo(message)
      if (success) {
        activeMasterEndpoint = master
        submittedDriverID = driverId.get
      } else if (!Utils.responseFromBackup(message)) {
        System.exit(-1)
      }

    case KillDriverResponse(master, driverId, success, message) =>
      logInfo(message)
      if (success) {
        activeMasterEndpoint = master
      } else if (!Utils.responseFromBackup(message)) {
        System.exit(-1)
      }

    case DriverStatusResponse(found, state, workerId, workerHostPort, exception) =>
      reportDriverStatus(found, state, workerId, workerHostPort, exception)
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (!lostMasters.contains(remoteAddress)) {
      logError(s"Error connecting to master $remoteAddress.")
      lostMasters += remoteAddress
      // Note that this heuristic does not account for the fact that a Master can recover within
      // the lifetime of this client. Thus, once a Master is lost it is lost to us forever. This
      // is not currently a concern, however, because this client does not retry submissions.
      if (lostMasters.size >= masterEndpoints.size) {
        logError("No master is available, exiting.")
        System.exit(-1)
      }
    }
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    if (!lostMasters.contains(remoteAddress)) {
      logError(s"Error connecting to master ($remoteAddress).")
      logError(s"Cause was: $cause")
      lostMasters += remoteAddress
      if (lostMasters.size >= masterEndpoints.size) {
        logError("No master is available, exiting.")
        System.exit(-1)
      }
    }
  }

  override def onError(cause: Throwable): Unit = {
    logError(s"Error processing messages, exiting.")
    cause.printStackTrace()
    System.exit(-1)
  }

  override def onStop(): Unit = {
    forwardMessageThread.shutdownNow()
  }
}

/**
 * Executable utility for starting and terminating drivers inside of a standalone cluster.
 */
object Client {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    // 若sys中不包含SPARK_SUBMIT，则打印警告信息
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a future version of Spark")
      println("Use ./bin/spark-submit with \"--master spark://host:port\"")
    }
    // scalastyle:on println
    new ClientApp().start(args, new SparkConf())
  }
}

private[spark] class ClientApp extends SparkApplication {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    // 实例化出一个SparkConfig对象，通过这个配置对象，可以在代码中指定一些配置项，
    // 如appName、Master地址等。val driverArgs = new ClientArguments(args)
    // 使用传入的args参数构建一个ClientArguments对象，该对象同样保留传入的配置信息，
    // 如Executor memory、Executor cores等都包含在这个
    val driverArgs = new ClientArguments(args)
    // 设置RPC请求超时时间为10sif (!conf.contains("spark.rpc.
    if (!conf.contains(RPC_ASK_TIMEOUT)) {
      conf.set(RPC_ASK_TIMEOUT, "10s")
    }
    Logger.getRootLogger.setLevel(driverArgs.logLevel)
    // 使用RpcEnv的create创建RPC环境
//    使用该成员设置好到Master的通信端点，通过该端点实现同Master的通信
    val rpcEnv =
      RpcEnv.create("driverClient", Utils.localHostName(), 0, conf, new SecurityManager(conf))
    // 得到master的URL并得到Master的Endpoints，用于同Master通信
    val masterEndpoints = driverArgs.masters.map(RpcAddress.fromSparkURL).
      map(rpcEnv.setupEndpointRef(_, Master.ENDPOINT_NAME))
    // 在rpcEnv.setupEndpoint方法中调用new()函数创建一个Driver ClientEndpoint。
    // ClientEndpoint是一个ThreadSafeRpcEndpoint消息循环体，
    // 至此就生成了Driver ClientEndpoint。
    // 在ClientEndpoint的onStart方法中向Master提交注册。
    // 这里通过masterEndpoint向Master发送RequestSubmitDriver(driverDescription)请求，
    // 完成Driver的注册

    // Master收到Driver ClientEndpoint的RequestSubmitDriver消息以后，就将Driver的信息加入到waitingDrivers和drivers的数据结构中。
    // 然后进行schedule()资源分配，Master向Worker发送LaunchDriver的消息指令


    rpcEnv.setupEndpoint("client", new ClientEndpoint(rpcEnv, driverArgs, masterEndpoints, conf))
    // 等待rpcEnv的终止
    rpcEnv.awaitTermination()
  }

}
