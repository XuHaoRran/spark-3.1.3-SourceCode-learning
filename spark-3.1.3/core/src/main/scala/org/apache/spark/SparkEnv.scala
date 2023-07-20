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

package org.apache.spark

import java.io.File
import java.net.Socket
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.collection.mutable
import scala.util.Properties

import com.google.common.cache.CacheBuilder
import org.apache.hadoop.conf.Configuration

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.network.netty.{NettyBlockTransferService, SparkTransportConf}
import org.apache.spark.network.shuffle.ExternalBlockStoreClient
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage._
import org.apache.spark.util.{RpcUtils, Utils}

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, RpcEnv, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val serializerManager: SerializerManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val metricsSystem: MetricsSystem,
    val memoryManager: MemoryManager,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf) extends Logging {

  @volatile private[spark] var isStopped = false
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata =
    CacheBuilder.newBuilder().softValues().build[String, AnyRef]().asMap()

  private[spark] var driverTmpDir: Option[String] = None

  private[spark] def stop(): Unit = {

    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      mapOutputTracker.stop()
      shuffleManager.stop()
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()

      // If we only stop sc, but the driver process still run as a services then we need to delete
      // the tmp dir, if not, it will create too many tmp dirs.
      // We only need to delete the tmp dir create by driver
      driverTmpDir match {
        case Some(path) =>
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(s"Exception while deleting Spark temp dir: $path", e)
          }
        case None => // We just need to delete tmp dir created by driver, so do nothing on executor
      }
    }
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String,
      envVars: Map[String, String], worker: Socket): Unit = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark]
  def releasePythonWorker(pythonExec: String,
      envVars: Map[String, String], worker: Socket): Unit = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverSystemName = "sparkDriver"
  private[spark] val executorSystemName = "sparkExecutor"

  def set(e: SparkEnv): Unit = {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  /**
   * Create a SparkEnv for the driver.
   */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains(DRIVER_HOST_ADDRESS),
      s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    assert(conf.contains(DRIVER_PORT), s"${DRIVER_PORT.key} is not set on the driver!")
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
    val port = conf.get(DRIVER_PORT)
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(conf))
    } else {
      None
    }
    // 调用了create方法
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      bindAddress,
      advertiseAddress,
      Option(port),
      isLocal,
      numCores,
      ioEncryptionKey,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
   * Create a SparkEnv for an executor.
   * In coarse-grained mode, the executor provides an RpcEnv that is already instantiated.
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      hostname: String,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv = {
    val env = create(
      conf,
      executorId,
      bindAddress,
      hostname,
      None,
      isLocal,
      numCores,
      ioEncryptionKey
    )
    SparkEnv.set(env)
    env
  }

  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv = {
    createExecutorEnv(conf, executorId, hostname,
      hostname, numCores, ioEncryptionKey, isLocal)
  }

  /**
   * Helper method to create a SparkEnv for a driver or an executor.
   *
   * 在SparkEnv创建的主要对象比如是
   * rpcEnv,
   * serializer,
   * closureSerializer,
   * serializerManager,
   * mapOutputTracker,
   * shuffleManager,
   * broadcastManager,
   * blockManager,
   * securityManager,
   * metricsSystem,
   * memoryManager,
   * outputCommitCoordinator,
   */
  private def create(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Option[Int],
      isLocal: Boolean,
      numUsableCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    // 判断是否Driver
    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

    // Listener bus is only used on the driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }
    val authSecretFileConf = if (isDriver) AUTH_SECRET_FILE_DRIVER else AUTH_SECRET_FILE_EXECUTOR
    val securityManager = new SecurityManager(conf, ioEncryptionKey, authSecretFileConf)
    if (isDriver) {
      securityManager.initializeAuth()
    }

    ioEncryptionKey.foreach { _ =>
      if (!securityManager.isEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    val systemName = if (isDriver) driverSystemName else executorSystemName
    // 这里调用了RpcEnv.create
    val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port.getOrElse(-1), conf,
      securityManager, numUsableCores, !isDriver)

    // Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
    // 弄清楚在原始端口为0或已被占用的情况下，RpcEnv实际绑定到哪个端口。
    if (isDriver) {
      conf.set(DRIVER_PORT, rpcEnv.address.port)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property
    // if the property is not set, possibly initializing it with our conf
    // 创建一个由给定SparkConf属性命名的类的实例，如果该属性没有设置，可能会使用我们的conf初始化它
    def instantiateClassFromConf[T](propertyName: ConfigEntry[String]): T = {
      instantiateClass[T](conf.get(propertyName))
    }

    val serializer = instantiateClassFromConf[Serializer](SERIALIZER)
    logDebug(s"Using serializer: ${serializer.getClass}")

    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

    val closureSerializer = new JavaSerializer(conf)

    def registerOrLookupEndpoint(
        name: String, endpointCreator: => RpcEndpoint):
      RpcEndpointRef = {
      if (isDriver) {
        logInfo("Registering " + name)
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }
    // BroadcastManager是用来管理Broadcast的，该实例对象是在SparkContext创建SparkEnv的时候创建的。
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)
    // 跟踪所有的Mapper的输出
    // 如果是driver的话，就创建MapOutputTrackerMaster，否则创建MapOutputTrackerWorker
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
    // requires the MapOutputTracker itself
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
    // 用户可以通过短格式的命名来指定所使用的ShuffleManager
    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    // Spark 2.0版本默认是SortShuffleManager。Tungsten-Sort与Sort类似，
    // SortShuffleManager默认对数据进行排序，因此如果用户的业务逻辑中需要该排序机制，
    // 则使用默认的SortShuffle-Manager；
    // 如果需要使用Tungsten-Sort，则把Spark.Shuffle.manager设置成Tungsten-Sort
    // 。
    val shuffleMgrName = conf.get(config.SHUFFLE_MANAGER)
    val shuffleMgrClass =
      shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)

    // ShuffleManager是一个用于shuffle系统的可插拔接口。在Driver端SparkEnv中创建ShuffleManager，在每个Executor上也会创建。基于spark.shuffle.manager进行设置。
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)
    // 创建memoryManager，是UnifiedMemoryManager
    val memoryManager: MemoryManager = UnifiedMemoryManager(conf, numUsableCores)

    val blockManagerPort = if (isDriver) {
      conf.get(DRIVER_BLOCK_MANAGER_PORT)
    } else {
      conf.get(BLOCK_MANAGER_PORT)
    }

    val externalShuffleClient = if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
      val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
      Some(new ExternalBlockStoreClient(transConf, securityManager,
        securityManager.isAuthenticationEnabled(), conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT)))
    } else {
      None
    }

    // Mapping from block manager id to the block manager's information.
    // 1.在Driver中是通过BlockManagerInfo来管理集群中每个ExecutorBackend中的BlockManager中的元数据信息的
    // 2.当改变了具体的ExecutorBackend上的Block信息后，就必须发消息给Driver中的BlockManagerMaster来更新相应的BlockManagerInfo
    // 3.当执行第二个Stage的时候，第二个Stage会向Driver中的MapOutputTracker-MasterEndpoint发消息请求上一个Stage中相应的输出，
    // 此时MapOutputTrackerMaster会把上一个Stage的输出数据的元数据信息发送给当前请求的Stage
    // 。
    val blockManagerInfo = new concurrent.TrieMap[BlockManagerId, BlockManagerInfo]()
    // 创建BlockManagerMasterEndpoint
    // blockManagerMaster主要对内提供各节点之间的指令通信服
    // BlockManagerMaster对整个集群的Block数据进行管理，Block是Spark数据管理的单位，与数据存储没有关系，
    // 数据可能存在磁盘上，也可能存储在内存中，还可能存储在offline，如Alluxio上的了啊
    val blockManagerMaster = new BlockManagerMaster(
      registerOrLookupEndpoint(
        BlockManagerMaster.DRIVER_ENDPOINT_NAME,
        new BlockManagerMasterEndpoint(
          rpcEnv,
          isLocal,
          conf,
          listenerBus,
          if (conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)) {
            externalShuffleClient
          } else {
            None
          }, blockManagerInfo,
          mapOutputTracker.asInstanceOf[MapOutputTrackerMaster])),
      registerOrLookupEndpoint(
        BlockManagerMaster.DRIVER_HEARTBEAT_ENDPOINT_NAME,
        new BlockManagerMasterHeartbeatEndpoint(rpcEnv, isLocal, blockManagerInfo)),
      conf,
      // isDriver判断是否运行在Driver上的了啊
      isDriver)

    val blockTransferService =
      new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress,
        blockManagerPort, numUsableCores, blockManagerMaster.driverEndpoint)

    // NB: blockManager is not valid until initialize() is called later.
    // 创建BlockManager，主要对外提供统一的访问接口
    // 注：blockManager无效，直到initialize（）被调用
    // 这里的BlockManagerMaster和BlockManager属于聚合关系
    val blockManager = new BlockManager(
      executorId,
      rpcEnv,
      blockManagerMaster,
      serializerManager,
      conf,
      memoryManager,
      mapOutputTracker,
      // shuffleManager是在SparkEnv中创建的，将shuffleManager传入到BlockManager，BlockManager就拥有shuffleManager的成员变量，
      // 从而可以调用shuffleManager的相关方法
      shuffleManager,
      blockTransferService,
      securityManager,
      externalShuffleClient)

    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem(MetricsSystemInstances.DRIVER, conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set(EXECUTOR_ID, executorId)
      val ms = MetricsSystem.createMetricsSystem(MetricsSystemInstances.EXECUTOR, conf,
        securityManager)
      ms.start(conf.get(METRICS_STATIC_SOURCES_ENABLED))
      ms
    }

    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      memoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) {
      val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
      envInstance.driverTmpDir = Some(sparkFilesDir)
    }

    envInstance
  }

  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
   */
  private[spark]
  def environmentDetails(
      conf: SparkConf,
      hadoopConf: Configuration,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String],
      addedArchives: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains(SCHEDULER_MODE)) {
        Seq((SCHEDULER_MODE.key, schedulingMode))
      } else {
        Seq.empty[(String, String)]
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles ++ addedArchives).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    // Add Hadoop properties, it will not ignore configs including in Spark. Some spark
    // conf starting with "spark.hadoop" may overwrite it.
    val hadoopProperties = hadoopConf.asScala
      .map(entry => (entry.getKey, entry.getValue)).toSeq.sorted
    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "Hadoop Properties" -> hadoopProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
