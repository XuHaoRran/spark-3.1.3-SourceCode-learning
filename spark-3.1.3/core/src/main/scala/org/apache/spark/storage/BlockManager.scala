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

package org.apache.spark.storage

import java.io._
import java.lang.ref.{ReferenceQueue => JReferenceQueue, WeakReference}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.Collections
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}
import scala.util.control.NonFatal

import com.codahale.metrics.{MetricRegistry, MetricSet}
import com.google.common.cache.CacheBuilder
import org.apache.commons.io.IOUtils

import org.apache.spark._
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Network
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.metrics.source.Source
import org.apache.spark.network._
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle._
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.util.TransportConf
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{MigratableResolver, ShuffleManager, ShuffleWriteMetricsReporter}
import org.apache.spark.storage.BlockManagerMessages.{DecommissionBlockManager, ReplicateBlock}
import org.apache.spark.storage.memory._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/* Class for returning a fetched block and associated metrics. */
private[spark] class BlockResult(
    val data: Iterator[Any],
    val readMethod: DataReadMethod.Value,
    val bytes: Long)

/**
 * Abstracts away how blocks are stored and provides different ways to read the underlying block
 * data. Callers should call [[dispose()]] when they're done with the block.
 */
private[spark] trait BlockData {

  def toInputStream(): InputStream

  /**
   * Returns a Netty-friendly wrapper for the block's data.
   *
   * Please see `ManagedBuffer.convertToNetty()` for more details.
   */
  def toNetty(): Object

  def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer

  def toByteBuffer(): ByteBuffer

  def size: Long

  def dispose(): Unit

}

private[spark] class ByteBufferBlockData(
    val buffer: ChunkedByteBuffer,
    val shouldDispose: Boolean) extends BlockData {

  override def toInputStream(): InputStream = buffer.toInputStream(dispose = false)

  override def toNetty(): Object = buffer.toNetty

  override def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    buffer.copy(allocator)
  }

  override def toByteBuffer(): ByteBuffer = buffer.toByteBuffer

  override def size: Long = buffer.size

  override def dispose(): Unit = {
    if (shouldDispose) {
      buffer.dispose()
    }
  }

}

private[spark] class HostLocalDirManager(
    futureExecutionContext: ExecutionContext,
    cacheSize: Int,
    blockStoreClient: BlockStoreClient) extends Logging {

  private val executorIdToLocalDirsCache =
    CacheBuilder
      .newBuilder()
      .maximumSize(cacheSize)
      .build[String, Array[String]]()

  private[spark] def getCachedHostLocalDirs: Map[String, Array[String]] =
    executorIdToLocalDirsCache.synchronized {
      executorIdToLocalDirsCache.asMap().asScala.toMap
    }

  private[spark] def getHostLocalDirs(
      host: String,
      port: Int,
      executorIds: Array[String])(
      callback: Try[Map[String, Array[String]]] => Unit): Unit = {
    val hostLocalDirsCompletable = new CompletableFuture[java.util.Map[String, Array[String]]]
    blockStoreClient.getHostLocalDirs(
      host,
      port,
      executorIds,
      hostLocalDirsCompletable)
    hostLocalDirsCompletable.whenComplete { (hostLocalDirs, throwable) =>
      if (hostLocalDirs != null) {
        callback(Success(hostLocalDirs.asScala.toMap))
        executorIdToLocalDirsCache.synchronized {
          executorIdToLocalDirsCache.putAll(hostLocalDirs)
        }
      } else {
        callback(Failure(throwable))
      }
    }
  }
}

/**
 * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 *
 * Note that [[initialize()]] must be called before the BlockManager is usable.
 * 在每个节点（driver 和 excutors）上运行的manager，它提供了将块本地和远程放入各种存储（内存、磁盘和堆外）并检索块的接口。
 * 请注意，在BlockManager可用之前，必须调用（）
 *
 * BlockManager是管理整个Spark运行时数据的读写，包含数据存储本身，在数据存储的基础上进行数据读写。
 * 由于Spark是分布式的，所以BlockManager也是分布式的，BlockManager本身相对而言是一个比较大的模块，
 * Spark中有非常多的模块：调度模块、资源管理模块等。BlockManager是另外一个非常重要的模块。
 * BlockManager本身的源码量非常大。
 */
private[spark] class BlockManager(
    val executorId: String,
    rpcEnv: RpcEnv,
    val master: BlockManagerMaster, // BlockManagerMaster对整个集群的BlockManagerMaster进行管理
    val serializerManager: SerializerManager, // 默认的序列化器
    val conf: SparkConf,
    memoryManager: MemoryManager, // 内存管理
    mapOutputTracker: MapOutputTracker, // shuffle输出的时候，要记录shufflemaptask的输出位置，以供下一个Stage使用，因此需要进行记录
    shuffleManager: ShuffleManager,
    val blockTransferService: BlockTransferService, // 是进行网络操作的，如果要联通另外一个BlockManger进行数据读写操作，就需要BlockTransferService

    securityManager: SecurityManager, // 安全管理
    externalBlockStoreClient: Option[ExternalBlockStoreClient])
  extends BlockDataManager with BlockEvictionHandler with Logging {

  // same as `conf.get(config.SHUFFLE_SERVICE_ENABLED)`
  private[spark] val externalShuffleServiceEnabled: Boolean = externalBlockStoreClient.isDefined

  private val remoteReadNioBufferConversion =
    conf.get(Network.NETWORK_REMOTE_READ_NIO_BUFFER_CONVERSION)

  private[spark] val subDirsPerLocalDir = conf.get(config.DISKSTORE_SUB_DIRECTORIES)

  // 管理磁盘的读写，创建并维护磁盘上逻辑块和物理块之间的逻辑映射位置
  // 一个block被映射到根据BlockId生成的一个文件，块文件哈系列在目录spark.local.dir中
  // 或在目录SPARK LOCAL DIRS中
  val diskBlockManager = {
    // Only perform cleanup if an external service is not serving our shuffle files.
    val deleteFilesOnStop =
      !externalShuffleServiceEnabled || executorId == SparkContext.DRIVER_IDENTIFIER
    new DiskBlockManager(conf, deleteFilesOnStop)
  }

  // Visible for testing
  private[storage] val blockInfoManager = new BlockInfoManager

  // 创建一个缓存池
  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  // Actual storage of where blocks are kept
  private[spark] val memoryStore =
    new MemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)
  private[spark] val diskStore = new DiskStore(conf, diskBlockManager, securityManager)
  memoryManager.setMemoryStore(memoryStore)

  // Note: depending on the memory manager, `maxMemory` may actually vary over time.
  // However, since we use this only for reporting and logging, what we actually want here is
  // the absolute maximum value that `maxMemory` can ever possibly reach. We may need
  // to revisit whether reporting this value as the "max" is intuitive to the user.
  private val maxOnHeapMemory = memoryManager.maxOnHeapStorageMemory
  private val maxOffHeapMemory = memoryManager.maxOffHeapStorageMemory

  private[spark] val externalShuffleServicePort = StorageUtils.externalShuffleServicePort(conf)
  // BlockManagerId是BlockManager的唯一标识信息，BlockId是数据块的唯一信息，
  // 对应的Seq[(BlockId, Long)]表示一组数据块标识ID及其数据块大小的元组信息
  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' blocks. This is either an external service, or just the
  // standard BlockTransferService to directly connect to other Executors.
  private[spark] val blockStoreClient = externalBlockStoreClient.getOrElse(blockTransferService)

  // Max number of failures before this block manager refreshes the block locations from the driver
  private val maxFailuresBeforeLocationRefresh =
    conf.get(config.BLOCK_FAILURES_BEFORE_LOCATION_REFRESH)

  private val storageEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerStorageEndpoint(rpcEnv, this, mapOutputTracker))

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  // Field related to peer block managers that are necessary for block replication
  @volatile private var cachedPeers: Seq[BlockManagerId] = _
  private val peerFetchLock = new Object
  private var lastPeerFetchTimeNs = 0L

  private var blockReplicationPolicy: BlockReplicationPolicy = _

  // visible for test
  // This is volatile since if it's defined we should not accept remote blocks.
  @volatile private[spark] var decommissioner: Option[BlockManagerDecommissioner] = None

  // A DownloadFileManager used to track all the files of remote blocks which are above the
  // specified memory threshold. Files will be deleted automatically based on weak reference.
  // Exposed for test
  private[storage] val remoteBlockTempFileManager =
    new BlockManager.RemoteBlockDownloadFileManager(this)
  private val maxRemoteBlockToMem = conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)

  var hostLocalDirManager: Option[HostLocalDirManager] = None

  @inline final private def isDecommissioning() = {
    decommissioner.isDefined
  }

  @inline final private def checkShouldStore(blockId: BlockId) = {
    // Don't reject broadcast blocks since they may be stored during task exec and
    // don't need to be migrated.
    if (isDecommissioning() && !blockId.isBroadcast) {
        throw new BlockSavedOnDecommissionedBlockManagerException(blockId)
    }
  }

  // This is a lazy val so someone can migrating RDDs even if they don't have a MigratableResolver
  // for shuffles. Used in BlockManagerDecommissioner & block puts.
  private[storage] lazy val migratableResolver: MigratableResolver = {
    shuffleManager.shuffleBlockResolver.asInstanceOf[MigratableResolver]
  }

  override def getLocalDiskDirs: Array[String] = diskBlockManager.localDirsString

  /**
   * Abstraction for storing blocks from bytes, whether they start in memory or on disk.
   *
   * @param blockSize the decrypted size of the block
   */
  private[spark] abstract class BlockStoreUpdater[T](
      blockSize: Long,
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean,
      keepReadLock: Boolean) {

    /**
     *  Reads the block content into the memory. If the update of the block store is based on a
     *  temporary file this could lead to loading the whole file into a ChunkedByteBuffer.
     */
    protected def readToByteBuffer(): ChunkedByteBuffer

    protected def blockData(): BlockData

    protected def saveToDiskStore(): Unit

    private def saveDeserializedValuesToMemoryStore(inputStream: InputStream): Boolean = {
      try {
        val values = serializerManager.dataDeserializeStream(blockId, inputStream)(classTag)
        memoryStore.putIteratorAsValues(blockId, values, classTag) match {
          case Right(_) => true
          case Left(iter) =>
            // If putting deserialized values in memory failed, we will put the bytes directly
            // to disk, so we don't need this iterator and can close it to free resources
            // earlier.
            iter.close()
            false
        }
      } finally {
        IOUtils.closeQuietly(inputStream)
      }
    }

    private def saveSerializedValuesToMemoryStore(bytes: ChunkedByteBuffer): Boolean = {
      val memoryMode = level.memoryMode
      memoryStore.putBytes(blockId, blockSize, memoryMode, () => {
        if (memoryMode == MemoryMode.OFF_HEAP && bytes.chunks.exists(!_.isDirect)) {
          bytes.copy(Platform.allocateDirectBuffer)
        } else {
          bytes
        }
      })
    }

    /**
     * Put the given data according to the given level in one of the block stores, replicating
     * the values if necessary.
     *
     * If the block already exists, this method will not overwrite it.
     *
     * If keepReadLock is true, this method will hold the read lock when it returns (even if the
     * block already exists). If false, this method will hold no locks when it returns.
     *
     * @return true if the block was already present or if the put succeeded, false otherwise.
     */
     def save(): Boolean = {
      doPut(blockId, level, classTag, tellMaster, keepReadLock) { info =>
        val startTimeNs = System.nanoTime()

        // Since we're storing bytes, initiate the replication before storing them locally.
        // This is faster as data is already serialized and ready to send.
        val replicationFuture = if (level.replication > 1) {
          Future {
            // This is a blocking action and should run in futureExecutionContext which is a cached
            // thread pool.
            replicate(blockId, blockData(), level, classTag)
          }(futureExecutionContext)
        } else {
          null
        }
        if (level.useMemory) {
          // Put it in memory first, even if it also has useDisk set to true;
          // We will drop it to disk later if the memory store can't hold it.
          val putSucceeded = if (level.deserialized) {
            saveDeserializedValuesToMemoryStore(blockData().toInputStream())
          } else {
            saveSerializedValuesToMemoryStore(readToByteBuffer())
          }
          if (!putSucceeded && level.useDisk) {
            logWarning(s"Persisting block $blockId to disk instead.")
            saveToDiskStore()
          }
        } else if (level.useDisk) {
          saveToDiskStore()
        }
        val putBlockStatus = getCurrentBlockStatus(blockId, info)
        val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
        if (blockWasSuccessfullyStored) {
          // Now that the block is in either the memory or disk store,
          // tell the master about it.
          info.size = blockSize
          if (tellMaster && info.tellMaster) {
            reportBlockStatus(blockId, putBlockStatus)
          }
          addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        }
        logDebug(s"Put block ${blockId} locally took ${Utils.getUsedTimeNs(startTimeNs)}")
        if (level.replication > 1) {
          // Wait for asynchronous replication to finish
          try {
            ThreadUtils.awaitReady(replicationFuture, Duration.Inf)
          } catch {
            case NonFatal(t) =>
              throw new SparkException("Error occurred while waiting for replication to finish", t)
          }
        }
        if (blockWasSuccessfullyStored) {
          None
        } else {
          Some(blockSize)
        }
      }.isEmpty
    }
  }

  /**
   * Helper for storing a block from bytes already in memory.
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   */
  private case class ByteBufferBlockStoreUpdater[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      bytes: ChunkedByteBuffer,
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false)
    extends BlockStoreUpdater[T](bytes.size, blockId, level, classTag, tellMaster, keepReadLock) {

    override def readToByteBuffer(): ChunkedByteBuffer = bytes

    /**
     * The ByteBufferBlockData wrapper is not disposed of to avoid releasing buffers that are
     * owned by the caller.
     */
    override def blockData(): BlockData = new ByteBufferBlockData(bytes, false)

    override def saveToDiskStore(): Unit = diskStore.putBytes(blockId, bytes)

  }

  /**
   * Helper for storing a block based from bytes already in a local temp file.
   */
  private[spark] case class TempFileBasedBlockStoreUpdater[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      tmpFile: File,
      blockSize: Long,
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false)
    extends BlockStoreUpdater[T](blockSize, blockId, level, classTag, tellMaster, keepReadLock) {

    override def readToByteBuffer(): ChunkedByteBuffer = {
      val allocator = level.memoryMode match {
        case MemoryMode.ON_HEAP => ByteBuffer.allocate _
        case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
      }
      blockData().toChunkedByteBuffer(allocator)
    }

    override def blockData(): BlockData = diskStore.getBytes(tmpFile, blockSize)

    override def saveToDiskStore(): Unit = diskStore.moveFileToBlock(tmpFile, blockSize, blockId)

    override def save(): Boolean = {
      val res = super.save()
      tmpFile.delete()
      res
    }

  }

  /**
   * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
   *
   * This method initializes the BlockTransferService and BlockStoreClient, registers with the
   * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
   * service if configured.
   *
   * 在BlockManager的initialize方法上右击Find Usages，可以看到initialize方法在两个地方得到调用：一个是SparkContext；
   * 另一个是Executor。启动Executor时，会调用BlockManager的initialize方法。
   */
  def initialize(appId: String /** 应用id **/): Unit = {
    // 调用blockTransferService的init方法，blockTransferService用于在不同节点fetch数据，传送数据
    // 初始化BlockTransferService
    blockTransferService.init(this)
    externalBlockStoreClient.foreach { blockStoreClient =>
        // 用于读取其他Executor上的shuffle files
        // 初始化ShuffleClient
      blockStoreClient.init(appId)
    }
    blockReplicationPolicy = {
      val priorityClass = conf.get(config.STORAGE_REPLICATION_POLICY)
      val clazz = Utils.classForName(priorityClass)
      val ret = clazz.getConstructor().newInstance().asInstanceOf[BlockReplicationPolicy]
      logInfo(s"Using $priorityClass for block replication policy")
      ret
    }

    // 创建BlockManagerId
    val id =
      BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)
    // 向blockManagerMaster注册BlockManager，在registerBlockManager方法中传入了slaveEndpoint，slaveEndpoint为BlockManager
    // 中的RPC对象，用于和blockManagerMasater通信
    val idFromMaster = master.registerBlockManager(
      id,
      diskBlockManager.localDirsString,
      maxOnHeapMemory,
      maxOffHeapMemory,
      storageEndpoint )  // 这个消息循环体来接收Driver中BlockManagerMaster的信息
    // 得到shuffleManageId
    blockManagerId = if (idFromMaster != null) idFromMaster else id
    // 得到shuffleServerId
    shuffleServerId = if (externalShuffleServiceEnabled) {
      logInfo(s"external shuffle service port = $externalShuffleServicePort")
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }

    // Register Executors' configuration with the local shuffle service, if one should exist.
    // 注册shuffleserver
    // 如果存在，将注册Executors配置与本地shuffle服务
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }

    hostLocalDirManager = {
      if (conf.get(config.SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED) &&
          !conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
        Some(new HostLocalDirManager(
          futureExecutionContext,
          conf.get(config.STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE),
          blockStoreClient))
      } else {
        None
      }
    }

    logInfo(s"Initialized BlockManager: $blockManagerId")
  }

  def shuffleMetricsSource: Source = {
    import BlockManager._

    if (externalShuffleServiceEnabled) {
      new ShuffleMetricsSource("ExternalShuffle", blockStoreClient.shuffleMetrics())
    } else {
      new ShuffleMetricsSource("NettyBlockTransfer", blockStoreClient.shuffleMetrics())
    }
  }

  private def registerWithExternalShuffleServer(): Unit = {
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirsString,
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.getClass.getName)

    val MAX_ATTEMPTS = conf.get(config.SHUFFLE_REGISTRATION_MAX_ATTEMPTS)
    val SLEEP_TIME_SECS = 5

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // Synchronous and will throw an exception if we cannot connect.
        blockStoreClient.asInstanceOf[ExternalBlockStoreClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000L)
        case NonFatal(e) =>
          throw new SparkException("Unable to register with external shuffle server due to : " +
            e.getMessage, e)
      }
    }
  }

  /**
   * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   *
   * This function deliberately fails silently if the master returns false (indicating that
   * the storage endpoint needs to re-register). The error condition will be detected again by the
   * next heart beat attempt or new block registration and another try to re-register all blocks
   * will be made then.
   *
   * 具体的Executor须向Driver不断地汇报自己的状态
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfoManager.size} blocks to the master.")
    for ((blockId, info) <- blockInfoManager.entries) {
      // reportAllBlocks方法中调用了getCurrentBlockStatus，包括内存、磁盘等信息
      val status = getCurrentBlockStatus(blockId, info)
      if (info.tellMaster && !tryToReportBlockStatus(blockId, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * Re-register with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block manager indicates that we were not registered.
   *
   * Note that this method must be called without any BlockInfo locks held.
   */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo(s"BlockManager $blockManagerId re-registering with master")
    master.registerBlockManager(blockManagerId, diskBlockManager.localDirsString, maxOnHeapMemory,
      maxOffHeapMemory, storageEndpoint)
    reportAllBlocks()
  }

  /**
   * Re-register with the master sometime soon.
   */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
  }

  /**
   * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      try {
        ThreadUtils.awaitReady(task, Duration.Inf)
      } catch {
        case NonFatal(t) =>
          throw new Exception("Error occurred while waiting for async. reregistration", t)
      }
    }
  }

  override def getHostLocalShuffleData(
      blockId: BlockId,
      dirs: Array[String]): ManagedBuffer = {
    shuffleManager.shuffleBlockResolver.getBlockData(blockId, Some(dirs))
  }

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   */
  override def getLocalBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {
      logDebug(s"Getting local shuffle block ${blockId}")
      try {
        shuffleManager.shuffleBlockResolver.getBlockData(blockId)
      } catch {
        case e: IOException =>
          if (conf.get(config.STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined) {
            FallbackStorage.read(conf, blockId)
          } else {
            throw e
          }
      }
    } else {
      getLocalBytes(blockId) match {
        case Some(blockData) =>
          new BlockManagerManagedBuffer(blockInfoManager, blockId, blockData, true)
        case None =>
          // If this block manager receives a request for a block that it doesn't have then it's
          // likely that the master has outdated block statuses for this block. Therefore, we send
          // an RPC so that this block is marked as being unavailable from this block manager.
          // 如果这个块管理器接收的请求是一个它没有的块，那么它很可能是主人对这个块的状态有过时的块。因此，
          // 我们发送一个RPC，以便这个块被标记为不可用的块管理器。
          reportBlockStatus(blockId, BlockStatus.empty)
          throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

  /**
   * Put the block locally, using the given storage level.
   *
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   */
  override def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean = {
    putBytes(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), level)(classTag)
  }

  override def putBlockDataAsStream(
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_]): StreamCallbackWithID = {

    checkShouldStore(blockId)

    if (blockId.isShuffle) {
      logDebug(s"Putting shuffle block ${blockId}")
      try {
        return migratableResolver.putShuffleBlockAsStream(blockId, serializerManager)
      } catch {
        case e: ClassCastException => throw new SparkException(
          s"Unexpected shuffle block ${blockId} with unsupported shuffle " +
          s"resolver ${shuffleManager.shuffleBlockResolver}")
      }
    }
    logDebug(s"Putting regular block ${blockId}")
    // All other blocks
    val (_, tmpFile) = diskBlockManager.createTempLocalBlock()
    val channel = new CountingWritableChannel(
      Channels.newChannel(serializerManager.wrapForEncryption(new FileOutputStream(tmpFile))))
    logTrace(s"Streaming block $blockId to tmp file $tmpFile")
    new StreamCallbackWithID {

      override def getID: String = blockId.name

      override def onData(streamId: String, buf: ByteBuffer): Unit = {
        while (buf.hasRemaining) {
          channel.write(buf)
        }
      }

      override def onComplete(streamId: String): Unit = {
        logTrace(s"Done receiving block $blockId, now putting into local blockManager")
        // Note this is all happening inside the netty thread as soon as it reads the end of the
        // stream.
        channel.close()
        val blockSize = channel.getCount
        val blockStored = TempFileBasedBlockStoreUpdater(
          blockId, level, classTag, tmpFile, blockSize).save()
        if (!blockStored) {
          throw new Exception(s"Failure while trying to store block $blockId on $blockManagerId.")
        }
      }

      override def onFailure(streamId: String, cause: Throwable): Unit = {
        // the framework handles the connection itself, we just need to do local cleanup
        channel.close()
        tmpFile.delete()
      }
    }
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * NOTE: This is mainly for testing.
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfoManager.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      BlockStatus(info.level, memSize = memSize, diskSize = diskSize)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the disk block manager (that the block manager
   * may not know of).
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    // The `toArray` is necessary here in order to force the list to be materialized so that we
    // don't try to serialize a lazy iterator when responding to client requests.
    (blockInfoManager.entries.map(_._1) ++ diskBlockManager.getAllBlocks())
      .filter(filter)
      .toArray
      .toSeq
  }

  /**
   * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   *
   * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
   * it is still valid). This ensures that update in master will compensate for the increase in
   * memory on the storage endpoint.
   */
  private[spark] def reportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit = {
    val needReregister = !tryToReportBlockStatus(blockId, status, droppedMemorySize)
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // Re-registering will report our new block for free.
      asyncReregister()
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
   * Actually send a UpdateBlockInfo message. Returns the master's response,
   * which will be true if the block was successfully recorded and false if
   * the storage endpoint needs to re-register.
   */
  private def tryToReportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean = {
    val storageLevel = status.storageLevel
    val inMemSize = Math.max(status.memSize, droppedMemorySize)
    val onDiskSize = status.diskSize
    master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
  }

  /**
   * Return the updated storage status of the block with the given ID. More specifically, if
   * the block is dropped from memory and possibly added to disk, return the new storage level
   * and the updated in-memory and on-disk sizes.
   */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      info.level match {
        case null =>
          BlockStatus.empty
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem  || onDisk) level.replication else 1
          val storageLevel = StorageLevel(
            useDisk = onDisk,
            useMemory = inMem,
            useOffHeap = level.useOffHeap,
            deserialized = deserialized,
            replication = replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          // 包含存储级别StorageLevel、内存大小、磁盘大小等信息。
          BlockStatus(storageLevel, memSize, diskSize)
      }
    }
  }

  /**
   * Get locations of an array of blocks.
   * 根据BlockId获取这个BlockId所在的BlockManager
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeNs = System.nanoTime()
    // 根据BlockId通过master.getLocations向Master获取位置信息，因为master管理所有的位置信息
    //
    // getLocations方法里的driverEndpoint是BlockManagerMasterEndpoint，Executor
    // 向BlockManagerMasterEndpoint发送GetLocationsMultipleBlockIds消息。
    val locations = master.getLocations(blockIds).toArray
    logDebug(s"Got multiple block location in ${Utils.getUsedTimeNs(startTimeNs)}")
    locations
  }

  /**
   * Cleanup code run in response to a failed local read.
   * Must be called while holding a read lock on the block.
   */
  private def handleLocalReadFailure(blockId: BlockId): Nothing = {
    releaseLock(blockId)
    // Remove the missing block so that its unavailability is reported to the driver
    removeBlock(blockId)
    throw new SparkException(s"Block $blockId was not found even though it's read-locked")
  }

  /**
   * Get block from local block manager as an iterator of Java objects.
   * 从blockinfomanager中获取本地数据
   */
  def getLocalValues(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    blockInfoManager.lockForReading(blockId) match {
      case None =>
        logDebug(s"Block $blockId was not found")
        None
      case Some(info) =>
        // 首先根据blockId从blockInfoManager中获取BlockInfo信息

        // 从BLockInfo信息获取level级别，
        val level = info.level
        logDebug(s"Level for block $blockId is $level")
        val taskContext = Option(TaskContext.get())
        // 根据level.useMemory && memoryStore.contains(blockId) 判断是否在内存中，如果在内存中，就从memoryStore中获取数据
        if (level.useMemory && memoryStore.contains(blockId)) {
          val iter: Iterator[Any] = if (level.deserialized) {
            memoryStore.getValues(blockId).get
          } else {
            serializerManager.dataDeserializeStream(
              blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)
          }
          // We need to capture the current taskId in case the iterator completion is triggered
          // from a different thread which does not have TaskContext set; see SPARK-18406 for
          // discussion.
          val ci = CompletionIterator[Any, Iterator[Any]](iter, {
            releaseLock(blockId, taskContext)
          })
          Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
        } else if (level.useDisk && diskStore.contains(blockId)) {
          // 根据level.useDisk && diskStore.contains(blockId)判断是否在磁盘中，如果在磁盘中，就从diskStore中获取数据
          val diskData = diskStore.getBytes(blockId)
          val iterToReturn: Iterator[Any] = {
            if (level.deserialized) {
              val diskValues = serializerManager.dataDeserializeStream(
                blockId,
                diskData.toInputStream())(info.classTag)
              maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
            } else {
              val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
                .map { _.toInputStream(dispose = false) }
                .getOrElse { diskData.toInputStream() }
              serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
            }
          }
          // 如果迭代器是从零一个没有TaskContext集的线程出发的，那么就需要捕获当前taskId
          val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, {
            releaseLockAndDispose(blockId, diskData, taskContext)
          })
          Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
        } else {
          handleLocalReadFailure(blockId)
        }
    }
  }

  /**
   * Get block from the local block manager as serialized bytes.
   */
  def getLocalBytes(blockId: BlockId): Option[BlockData] = {
    logDebug(s"Getting local block $blockId as bytes")
    assert(!blockId.isShuffle, s"Unexpected ShuffleBlockId $blockId")
    blockInfoManager.lockForReading(blockId).map { info => doGetLocalBytes(blockId, info) }
  }

  /**
   * Get block from the local block manager as serialized bytes.
   *
   * Must be called while holding a read lock on the block.
   * Releases the read lock upon exception; keeps the read lock upon successful return.
   */
  private def doGetLocalBytes(blockId: BlockId, info: BlockInfo): BlockData = {
    val level = info.level
    logDebug(s"Level for block $blockId is $level")
    // In order, try to read the serialized bytes from memory, then from disk, then fall back to
    // serializing in-memory objects, and, finally, throw an exception if the block does not exist.
    if (level.deserialized) {
      // Try to avoid expensive serialization by reading a pre-serialized copy from disk:
      if (level.useDisk && diskStore.contains(blockId)) {
        // Note: we purposely do not try to put the block back into memory here. Since this branch
        // handles deserialized blocks, this block may only be cached in memory as objects, not
        // serialized bytes. Because the caller only requested bytes, it doesn't make sense to
        // cache the block's deserialized objects since that caching may not have a payoff.
        diskStore.getBytes(blockId)
      } else if (level.useMemory && memoryStore.contains(blockId)) {
        // The block was not found on disk, so serialize an in-memory copy:
        new ByteBufferBlockData(serializerManager.dataSerializeWithExplicitClassTag(
          blockId, memoryStore.getValues(blockId).get, info.classTag), true)
      } else {
        handleLocalReadFailure(blockId)
      }
    } else {  // storage level is serialized
      if (level.useMemory && memoryStore.contains(blockId)) {
        new ByteBufferBlockData(memoryStore.getBytes(blockId).get, false)
      } else if (level.useDisk && diskStore.contains(blockId)) {
        val diskData = diskStore.getBytes(blockId)
        maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
          .map(new ByteBufferBlockData(_, false))
          .getOrElse(diskData)
      } else {
        handleLocalReadFailure(blockId)
      }
    }
  }

  /**
   * Get block from remote block managers.
   *
   * This does not acquire a lock on this block in this JVM.
   *
   * 获取远处数据
   */
  private[spark] def getRemoteValues[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val ct = implicitly[ClassTag[T]]
    // 获取远程的数据，如果获取的失败次数超过最大的获取次数（locations.size），就提示失败，返回空值；如果获取到远程数据，就返回
    getRemoteBlock(blockId, (data: ManagedBuffer) => {
      val values =
        serializerManager.dataDeserializeStream(blockId, data.createInputStream())(ct)
      new BlockResult(values, DataReadMethod.Network, data.size)
    })
  }

  /**
   * Get the remote block and transform it to the provided data type.
   *
   * If the block is persisted to the disk and stored at an executor running on the same host then
   * first it is tried to be accessed using the local directories of the other executor directly.
   * If the file is successfully identified then tried to be transformed by the provided
   * transformation function which expected to open the file. If there is any exception during this
   * transformation then block access falls back to fetching it from the remote executor via the
   * network.
   *
   * @param blockId identifies the block to get
   * @param bufferTransformer this transformer expected to open the file if the block is backed by a
   *                          file by this it is guaranteed the whole content can be loaded
   * @tparam T result type
   */
  private[spark] def getRemoteBlock[T](
      blockId: BlockId,
      bufferTransformer: ManagedBuffer => T): Option[T] = {
    logDebug(s"Getting remote block $blockId")
    require(blockId != null, "BlockId is null")

    // Because all the remote blocks are registered in driver, it is not necessary to ask
    // all the storage endpoints to get block status.
    // 因为所有的远程块都注册在driver中，所以不需要要求所有的从执行器获取块状态
    val locationsAndStatusOption = master.getLocationsAndStatus(blockId, blockManagerId.host)
    if (locationsAndStatusOption.isEmpty) {
      logDebug(s"Block $blockId is unknown by block manager master")
      None
    } else {
      val locationsAndStatus = locationsAndStatusOption.get
      val blockSize = locationsAndStatus.status.diskSize.max(locationsAndStatus.status.memSize)

      locationsAndStatus.localDirs.flatMap { localDirs =>
        val blockDataOption =
          readDiskBlockFromSameHostExecutor(blockId, localDirs, locationsAndStatus.status.diskSize)
        val res = blockDataOption.flatMap { blockData =>
          try {
            Some(bufferTransformer(blockData))
          } catch {
            case NonFatal(e) =>
              logDebug("Block from the same host executor cannot be opened: ", e)
              None
          }
        }
        logInfo(s"Read $blockId from the disk of a same host executor is " +
          (if (res.isDefined) "successful." else "failed."))
        res
      }.orElse {
        fetchRemoteManagedBuffer(blockId, blockSize, locationsAndStatus).map(bufferTransformer)
      }
    }
  }

  private def preferExecutors(locations: Seq[BlockManagerId]): Seq[BlockManagerId] = {
    val (executors, shuffleServers) = locations.partition(_.port != externalShuffleServicePort)
    executors ++ shuffleServers
  }

  /**
   * Return a list of locations for the given block, prioritizing the local machine since
   * multiple block managers can share the same host, followed by hosts on the same rack.
   *
   * Within each of the above listed groups (same host, same rack and others) executors are
   * preferred over the external shuffle service.
   */
  private[spark] def sortLocations(locations: Seq[BlockManagerId]): Seq[BlockManagerId] = {
    val locs = Random.shuffle(locations)
    val (preferredLocs, otherLocs) = locs.partition(_.host == blockManagerId.host)
    val orderedParts = blockManagerId.topologyInfo match {
      case None => Seq(preferredLocs, otherLocs)
      case Some(_) =>
        val (sameRackLocs, differentRackLocs) = otherLocs.partition {
          loc => blockManagerId.topologyInfo == loc.topologyInfo
        }
        Seq(preferredLocs, sameRackLocs, differentRackLocs)
    }
    orderedParts.map(preferExecutors).reduce(_ ++ _)
  }

  /**
   * Fetch the block from remote block managers as a ManagedBuffer.
   */
  private def fetchRemoteManagedBuffer(
      blockId: BlockId,
      blockSize: Long,
      locationsAndStatus: BlockManagerMessages.BlockLocationsAndStatus): Option[ManagedBuffer] = {
    // If the block size is above the threshold, we should pass our FileManger to
    // BlockTransferService, which will leverage it to spill the block; if not, then passed-in
    // null value means the block will be persisted in memory.
    // 如果块大小超过阈值，将FileManager传递给BlockTransferService，利用它来溢出块；如果没有，传递空值意味着块将持久存在内存中。
    val tempFileManager = if (blockSize > maxRemoteBlockToMem) {
      remoteBlockTempFileManager
    } else {
      null
    }
    var runningFailureCount = 0
    var totalFailureCount = 0
    // sortLocations方法返回给定块的位置列表，本地计算机的优先级从多个块管理器可以共享同一个主机，然后是同一机架上的主机
    val locations = sortLocations(locationsAndStatus.locations)
    val maxFetchFailures = locations.size
    var locationIterator = locations.iterator
    while (locationIterator.hasNext) {
      val loc = locationIterator.next()
      logDebug(s"Getting remote block $blockId from $loc")
      val data = try {
        // 调用blockTransferService.fetchBlockSync方法实现远程获取数据
        val buf = blockTransferService.fetchBlockSync(loc.host, loc.port, loc.executorId,
          blockId.toString, tempFileManager)
        if (blockSize > 0 && buf.size() == 0) {
          throw new IllegalStateException("Empty buffer received for non empty block")
        }
        buf
      } catch {
        case NonFatal(e) =>
          runningFailureCount += 1
          totalFailureCount += 1
          // 放弃尝试的位置。要么我们已经尝试了所有的原始位置,或者我们已经从master节点刷新了位置列表，并且仍然在刷新列表中尝试位置后命中失败
          if (totalFailureCount >= maxFetchFailures) {
            // Give up trying anymore locations. Either we've tried all of the original locations,
            // or we've refreshed the list of locations from the master, and have still
            // hit failures after trying locations from the refreshed list.
            logWarning(s"Failed to fetch block after $totalFailureCount fetch failures. " +
              s"Most recent failure cause:", e)
            return None
          }

          logWarning(s"Failed to fetch remote block $blockId " +
            s"from $loc (failed attempt $runningFailureCount)", e)

          // If there is a large number of executors then locations list can contain a
          // large number of stale entries causing a large number of retries that may
          // take a significant amount of time. To get rid of these stale entries
          // we refresh the block locations after a certain number of fetch failures
          // 如果有大量的 Executors，那么位置列表可以包含一个旧的条目造成大量重试，可能花费大量的时间。
          // 在一定数量的获取失败之后，为去掉这些旧的条目，我们刷新块位置
          if (runningFailureCount >= maxFailuresBeforeLocationRefresh) {
            // 如果有大量执行者，则位置列表可以包含大量过时的条目导致大量重试，可能花大量的时间。除去这些陈旧的条目，在一定数量的提取失败后刷新块位置
            locationIterator = sortLocations(master.getLocations(blockId)).iterator
            logDebug(s"Refreshed locations from the driver " +
              s"after ${runningFailureCount} fetch failures.")
            runningFailureCount = 0
          }

          // This location failed, so we retry fetch from a different one by returning null here
          // 此位置失败，所以我们尝试从不同的位置获取，这里返回一个 null
          null
      }

      if (data != null) {
        // If the ManagedBuffer is a BlockManagerManagedBuffer, the disposal of the
        // byte buffers backing it may need to be handled after reading the bytes.
        // In this case, since we just fetched the bytes remotely, we do not have
        // a BlockManagerManagedBuffer. The assert here is to ensure that this holds
        // true (or the disposal is handled).
        assert(!data.isInstanceOf[BlockManagerManagedBuffer])
        return Some(data)
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }

  /**
   * Reads the block from the local directories of another executor which runs on the same host.
   */
  private[spark] def readDiskBlockFromSameHostExecutor(
      blockId: BlockId,
      localDirs: Array[String],
      blockSize: Long): Option[ManagedBuffer] = {
    val file = ExecutorDiskUtils.getFile(localDirs, subDirsPerLocalDir, blockId.name)
    if (file.exists()) {
      val managedBuffer = securityManager.getIOEncryptionKey() match {
        case Some(key) =>
          // Encrypted blocks cannot be memory mapped; return a special object that does decryption
          // and provides InputStream / FileRegion implementations for reading the data.
          new EncryptedManagedBuffer(
            new EncryptedBlockData(file, blockSize, conf, key))

        case _ =>
          val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
          new FileSegmentManagedBuffer(transportConf, file, 0, file.length)
      }
      Some(managedBuffer)
    } else {
      None
    }
  }

  /**
   * Get block from remote block managers as serialized bytes.
   */
  def getRemoteBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    getRemoteBlock(blockId, (data: ManagedBuffer) => {
      // SPARK-24307 undocumented "escape-hatch" in case there are any issues in converting to
      // ChunkedByteBuffer, to go back to old code-path.  Can be removed post Spark 2.4 if
      // new path is stable.
      if (remoteReadNioBufferConversion) {
        new ChunkedByteBuffer(data.nioByteBuffer())
      } else {
        ChunkedByteBuffer.fromManagedBuffer(data)
      }
    })
  }

  /**
   * Get a block from the block manager (either local or remote).
   *
   * This acquires a read lock on the block if the block was stored locally and does not acquire
   * any locks if the block was fetched from a remote block manager. The read lock will
   * automatically be freed once the result's `data` iterator is fully consumed.
   */
  def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    // 如果数据在本地，get方法调用getLocalValues获取数据
    val local = getLocalValues(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemoteValues[T](blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  /**
   * Downgrades an exclusive write lock to a shared read lock.
   */
  def downgradeLock(blockId: BlockId): Unit = {
    blockInfoManager.downgradeLock(blockId)
  }

  /**
   * Release a lock on the given block with explicit TaskContext.
   * The param `taskContext` should be passed in case we can't get the correct TaskContext,
   * for example, the input iterator of a cached RDD iterates to the end in a child
   * thread.
   */
  def releaseLock(blockId: BlockId, taskContext: Option[TaskContext] = None): Unit = {
    val taskAttemptId = taskContext.map(_.taskAttemptId())
    // SPARK-27666. When a task completes, Spark automatically releases all the blocks locked
    // by this task. We should not release any locks for a task that is already completed.
    if (taskContext.isDefined && taskContext.get.isCompleted) {
      logWarning(s"Task ${taskAttemptId.get} already completed, not releasing lock for $blockId")
    } else {
      blockInfoManager.unlock(blockId, taskAttemptId)
    }
  }

  /**
   * Registers a task with the BlockManager in order to initialize per-task bookkeeping structures.
   */
  def registerTask(taskAttemptId: Long): Unit = {
    blockInfoManager.registerTask(taskAttemptId)
  }

  /**
   * Release all locks for the given task.
   *
   * @return the blocks whose locks were released.
   */
  def releaseAllLocksForTask(taskAttemptId: Long): Seq[BlockId] = {
    blockInfoManager.releaseAllLocksForTask(taskAttemptId)
  }

  /**
   * Retrieve the given block if it exists, otherwise call the provided `makeIterator` method
   * to compute the block, persist it, and return its values.
   *
   * @return either a BlockResult if the block was successfully cached, or an iterator if the block
   *         could not be cached.
   */
  def getOrElseUpdate[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      makeIterator: () => Iterator[T]): Either[BlockResult, Iterator[T]] = {
    // Attempt to read the block from local or remote storage. If it's present, then we don't need
    // to go through the local-get-or-put path.
    // 尝试从本地或远程存储读取块。如果它存在，那么就不需要通过本地get或put路径获取
    // 根据blockId调用了这个方法，get方法从block块manager（本地或远程）获取一个block，如果block在本地存储没且没获得锁，
    // 则先获取块block的读取锁， 如果该块是从远程块管理器获取的，当data迭代器被完全消费以后，那么读取锁将自动释放。
    // get的时候，如果本地有数据，从本地获取数据返回，如果没有数据，则从远程节点获取数据
    get[T](blockId)(classTag) match {
      case Some(block) =>
        return Left(block)
      case _ =>
            // 需要计算快
        // Need to compute the block.
    }
    // Initially we hold no locks on this block.
    // 需要计算blockInitially，在块上我们没有锁
    //
    // 没有获取到缓存数据， makeIterator就是getOrElseUpdate方法中传入的匿名函数，在匿名函数中获取到Iterator数据
    //
    // doPutIterator将makeIterator从父RDD的checkpoint读取的数据或者重新计算的数据存放到内存中，如果内存不够，就溢出到磁盘中持久化
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
      case None =>
        // doPut() didn't hand work back to us, so the block already existed or was successfully
        // stored. Therefore, we now hold a read lock on the block.
        // 读取锁
        // doput()方法没有返回，所以块已存在或者已成功存储。因此，我们现在在块上持有
        val blockResult = getLocalValues(blockId).getOrElse {
          // Since we held a read lock between the doPut() and get() calls, the block should not
          // have been evicted, so get() not returning the block indicates some internal error.
          //在 doPut()和 get()方法调用的时候，我们持有读取锁，块不应被驱逐，这样，get()方法没返回块，表示发生一些内部错误
          releaseLock(blockId)
          throw new SparkException(s"get() failed for block $blockId even though we held a lock")
        }
        // We already hold a read lock on the block from the doPut() call and getLocalValues()
        // acquires the lock again, so we need to call releaseLock() here so that the net number
        // of lock acquisitions is 1 (since the caller will only call release() once).
        // 我们已经持有调用 doPutO)方法在块上的读取锁，getLocalValues再一次获取锁，
        // 所以我们需要调用releaseLock(),这样获取锁的数量是1(因为调用者只releaseO)一次)
        releaseLock(blockId)
        Left(blockResult)
      case Some(iter) =>
        // The put failed, likely because the data was too large to fit in memory and could not be
        // dropped to disk. Therefore, we need to pass the input iterator back to the caller so
        // that they can decide what to do with the values (e.g. process them without caching).
        // 输入失败，可能是因为数据太大而不能存储在内存中，不能溢出到磁盘上。
        // 因此，我们需要将输入迭代器传递给调用者，他们可以决定如何处理这些值(例如，不缓存它们)
       Right(iter)
    }
  }

  /**
   * @return true if the block was stored or false if an error occurred.
   */
  def putIterator[T: ClassTag](
      blockId: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(values != null, "Values is null")
    doPutIterator(blockId, () => values, level, implicitly[ClassTag[T]], tellMaster) match {
      case None =>
        true
      case Some(iter) =>
        // Caller doesn't care about the iterator values, so we can close the iterator here
        // to free resources earlier
        iter.close()
        false
    }
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
   */
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetricsReporter): DiskBlockObjectWriter = {
    val syncWrites = conf.get(config.SHUFFLE_SYNC)
    new DiskBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,
      syncWrites, writeMetrics, blockId)
  }

  /**
   * Put a new block of serialized bytes to the block manager.
   *
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   *
   * @return true if the block was stored or false if an error occurred.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      bytes: ChunkedByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(bytes != null, "Bytes is null")
    val blockStoreUpdater =
      ByteBufferBlockStoreUpdater(blockId, level, implicitly[ClassTag[T]], bytes, tellMaster)
    blockStoreUpdater.save()
  }

  /**
   * Helper method used to abstract common code from [[BlockStoreUpdater.save()]]
   * and [[doPutIterator()]].
   *
   * @param putBody a function which attempts the actual put() and returns None on success
   *                or Some on failure.
   */
  private def doPut[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_],
      tellMaster: Boolean,
      keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")
    checkShouldStore(blockId)

    val putBlockInfo = {
      val newInfo = new BlockInfo(level, classTag, tellMaster)
      // lockNewBlockForWriting写入一个新的块前先尝试获得适当的锁，如果我们是第一个写块，获得写入锁后继续后续操作。
      // 否则，如果另一个线程已经写入块，须等待写入完成，才能获取读取锁，调用new()函数创建一个BlockInfo赋值给putBlockInfo，
      // 然后通过putBody(putBlockInfo)将数据存入。putBody是一个匿名函数，输入BlockInfo，输出的是一个泛型Option[T]。
      // putBody函数体内容是doPutIterator方法（doPutBytes方法也类似调用doPut）调用doPut时传入。
      if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {
        newInfo
      } else {
        logWarning(s"Block $blockId already exists on this machine; not re-adding it")
        if (!keepReadLock) {
          // lockNewBlockForWriting returned a read lock on the existing block, so we must free it:
          // 在现有的块上lockNewBlockForWriting，返回一个读锁，所以必须释放它
          releaseLock(blockId)
        }
        return None
      }
    }

    val startTimeNs = System.nanoTime()
    var exceptionWasThrown: Boolean = true
    val result: Option[T] = try {
      val res = putBody(putBlockInfo)
      exceptionWasThrown = false
      if (res.isEmpty) {
        // the block was successfully stored
        if (keepReadLock) {
          blockInfoManager.downgradeLock(blockId)
        } else {
          blockInfoManager.unlock(blockId)
        }
      } else {
        removeBlockInternal(blockId, tellMaster = false)
        logWarning(s"Putting block $blockId failed")
      }
      res
    } catch {
      // Since removeBlockInternal may throw exception,
      // we should print exception first to show root cause.
      case NonFatal(e) =>
        logWarning(s"Putting block $blockId failed due to exception $e.")
        throw e
    } finally {
      // This cleanup is performed in a finally block rather than a `catch` to avoid having to
      // catch and properly re-throw InterruptedException.
      if (exceptionWasThrown) {
        // If an exception was thrown then it's possible that the code in `putBody` has already
        // notified the master about the availability of this block, so we need to send an update
        // to remove this block location.
        removeBlockInternal(blockId, tellMaster = tellMaster)
        // The `putBody` code may have also added a new block status to TaskMetrics, so we need
        // to cancel that out by overwriting it with an empty block status. We only do this if
        // the finally block was entered via an exception because doing this unconditionally would
        // cause us to send empty block statuses for every block that failed to be cached due to
        // a memory shortage (which is an expected failure, unlike an uncaught exception).
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
      }
    }
    val usedTimeMs = Utils.getUsedTimeNs(startTimeNs)
    if (level.replication > 1) {
      logDebug(s"Putting block ${blockId} with replication took $usedTimeMs")
    } else {
      logDebug(s"Putting block ${blockId} without replication took ${usedTimeMs}")
    }
    result
  }

  /**
   * Put the given block according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * If the block already exists, this method will not overwrite it.
   *
   * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
   *                     block already exists). If false, this method will hold no locks when it
   *                     returns.
   * @return None if the block was already present or if the put succeeded, or Some(iterator)
   *         if the put failed.
   */
  private def doPutIterator[T](
      blockId: BlockId,
      iterator: () => Iterator[T],
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Option[PartiallyUnrolledIterator[T]] = {
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      val startTimeNs = System.nanoTime()
      var iteratorFromFailedMemoryStorePut: Option[PartiallyUnrolledIterator[T]] = None
      // Size of the block in bytes
      // 块的大小为字节
      var size = 0L
      if (level.useMemory) {
        // 首先把它放在内存中，即使useDisk 设置为 true;如果内存存储不能保存，我们
        // 稍后会把它放在磁盘上
        //
        // 如果level.useMemory，则在memoryStore中放入数据
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        //
        if (level.deserialized) {
          memoryStore.putIteratorAsValues(blockId, iterator(), classTag) match {
            case Right(s) =>
              size = s
            case Left(iter) =>
              // Not enough space to unroll this block; drop to disk if applicable
              // 没有足够的空间来展开块，如果使用，可以溢出到磁盘
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  serializerManager.dataSerializeStream(blockId, out, iter)(classTag)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(iter)
              }
          }
        } else { // !level.deserialized
          memoryStore.putIteratorAsBytes(blockId, iterator(), classTag, level.memoryMode) match {
            case Right(s) =>
              size = s
            case Left(partiallySerializedValues) =>
              // Not enough space to unroll this block; drop to disk if applicable
              // 没有足够的空间来展开块；如果使用，可以溢出到磁盘
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  partiallySerializedValues.finishWritingToStream(out)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(partiallySerializedValues.valuesIterator)
              }
          }
        }

      } else if (level.useDisk) {
        // 则在diskStore中放入数据
        diskStore.put(blockId) { channel =>
          val out = Channels.newOutputStream(channel)
          serializerManager.dataSerializeStream(blockId, out, iterator())(classTag)
        }
        size = diskStore.getSize(blockId)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        // Now that the block is in either the memory or disk store, tell the master about it.
        // 现在块位于内存或磁盘存储中，通知master
        info.size = size
        if (tellMaster && info.tellMaster) {
          reportBlockStatus(blockId, putBlockStatus)
        }
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        logDebug(s"Put block $blockId locally took ${Utils.getUsedTimeNs(startTimeNs)}")
        if (level.replication > 1) {
          // 在其他节点中存入副本数据
          val remoteStartTimeNs = System.nanoTime()
          val bytesToReplicate = doGetLocalBytes(blockId, info)
          // [SPARK-16550] Erase the typed classTag when using default serialization, since
          // NettyBlockRpcServer crashes when deserializing repl-defined classes.
          // TODO(ekl) remove this once the classloader issue on the remote end is fixed.
          val remoteClassTag = if (!serializerManager.canUseKryo(classTag)) {
            scala.reflect.classTag[Any]
          } else {
            classTag
          }
          try {
            replicate(blockId, bytesToReplicate, level, remoteClassTag)
          } finally {
            bytesToReplicate.dispose()
          }
          logDebug(s"Put block $blockId remotely took ${Utils.getUsedTimeNs(remoteStartTimeNs)}")
        }
      }
      assert(blockWasSuccessfullyStored == iteratorFromFailedMemoryStorePut.isEmpty)
      iteratorFromFailedMemoryStorePut
    }
  }

  /**
   * Attempts to cache spilled bytes read from disk into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the bytes from the memory store if the put succeeded, otherwise None.
   *         If this returns bytes from the memory store then the original disk store bytes will
   *         automatically be disposed and the caller should not continue to use them. Otherwise,
   *         if this returns None then the original disk store bytes will be unaffected.
   */
  private def maybeCacheDiskBytesInMemory(
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskData: BlockData): Option[ChunkedByteBuffer] = {
    require(!level.deserialized)
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          diskData.dispose()
          Some(memoryStore.getBytes(blockId).get)
        } else {
          val allocator = level.memoryMode match {
            case MemoryMode.ON_HEAP => ByteBuffer.allocate _
            case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
          }
          val putSucceeded = memoryStore.putBytes(blockId, diskData.size, level.memoryMode, () => {
            // https://issues.apache.org/jira/browse/SPARK-6076
            // If the file size is bigger than the free memory, OOM will happen. So if we
            // cannot put it into MemoryStore, copyForMemory should not be created. That's why
            // this action is put into a `() => ChunkedByteBuffer` and created lazily.
            diskData.toChunkedByteBuffer(allocator)
          })
          if (putSucceeded) {
            diskData.dispose()
            Some(memoryStore.getBytes(blockId).get)
          } else {
            None
          }
        }
      }
    } else {
      None
    }
  }

  /**
   * Attempts to cache spilled values read from disk into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the iterator. The original iterator passed this method should no longer
   *         be used after this method returns.
   */
  private def maybeCacheDiskValuesInMemory[T](
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskIterator: Iterator[T]): Iterator[T] = {
    require(level.deserialized)
    val classTag = blockInfo.classTag.asInstanceOf[ClassTag[T]]
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          // Note: if we had a means to discard the disk iterator, we would do that here.
          memoryStore.getValues(blockId).get
        } else {
          memoryStore.putIteratorAsValues(blockId, diskIterator, classTag) match {
            case Left(iter) =>
              // The memory store put() failed, so it returned the iterator back to us:
              iter
            case Right(_) =>
              // The put() succeeded, so we can read the values back:
              memoryStore.getValues(blockId).get
          }
        }
      }.asInstanceOf[Iterator[T]]
    } else {
      diskIterator
    }
  }

  /**
   * Get peer block managers in the system.
   */
  private[storage] def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      val cachedPeersTtl = conf.get(config.STORAGE_CACHED_PEERS_TTL) // milliseconds
      val diff = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastPeerFetchTimeNs)
      val timeout = diff > cachedPeersTtl
      if (cachedPeers == null || forceFetch || timeout) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTimeNs = System.nanoTime()
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      if (cachedPeers.isEmpty &&
          conf.get(config.STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined) {
        Seq(FallbackStorage.FALLBACK_BLOCK_MANAGER_ID)
      } else {
        cachedPeers
      }
    }
  }

  /**
   * Replicates a block to peer block managers based on existingReplicas and maxReplicas
   *
   * @param blockId blockId being replicate
   * @param existingReplicas existing block managers that have a replica
   * @param maxReplicas maximum replicas needed
   * @param maxReplicationFailures number of replication failures to tolerate before
   *                               giving up.
   * @return whether block was successfully replicated or not
   */
  def replicateBlock(
      blockId: BlockId,
      existingReplicas: Set[BlockManagerId],
      maxReplicas: Int,
      maxReplicationFailures: Option[Int] = None): Boolean = {
    logInfo(s"Using $blockManagerId to pro-actively replicate $blockId")
    blockInfoManager.lockForReading(blockId).forall { info =>
      val data = doGetLocalBytes(blockId, info)
      val storageLevel = StorageLevel(
        useDisk = info.level.useDisk,
        useMemory = info.level.useMemory,
        useOffHeap = info.level.useOffHeap,
        deserialized = info.level.deserialized,
        replication = maxReplicas)
      // we know we are called as a result of an executor removal or because the current executor
      // is getting decommissioned. so we refresh peer cache before trying replication, we won't
      // try to replicate to a missing executor/another decommissioning executor
      getPeers(forceFetch = true)
      try {
        replicate(
          blockId, data, storageLevel, info.classTag, existingReplicas, maxReplicationFailures)
      } finally {
        logDebug(s"Releasing lock for $blockId")
        releaseLockAndDispose(blockId, data)
      }
    }
  }

  /**
   * Replicate block to another node. Note that this is a blocking call that returns after
   * the block has been replicated.
   */
  private def replicate(
      blockId: BlockId,
      data: BlockData,
      level: StorageLevel,
      classTag: ClassTag[_],
      existingReplicas: Set[BlockManagerId] = Set.empty,
      maxReplicationFailures: Option[Int] = None): Boolean = {

    val maxReplicationFailureCount = maxReplicationFailures.getOrElse(
      conf.get(config.STORAGE_MAX_REPLICATION_FAILURE))
    val tLevel = StorageLevel(
      useDisk = level.useDisk,
      useMemory = level.useMemory,
      useOffHeap = level.useOffHeap,
      deserialized = level.deserialized,
      replication = 1)

    val numPeersToReplicateTo = level.replication - 1
    val startTime = System.nanoTime

    val peersReplicatedTo = mutable.HashSet.empty ++ existingReplicas
    val peersFailedToReplicateTo = mutable.HashSet.empty[BlockManagerId]
    var numFailures = 0

    val initialPeers = getPeers(false).filterNot(existingReplicas.contains)

    var peersForReplication = blockReplicationPolicy.prioritize(
      blockManagerId,
      initialPeers,
      peersReplicatedTo,
      blockId,
      numPeersToReplicateTo)

    while(numFailures <= maxReplicationFailureCount &&
      !peersForReplication.isEmpty &&
      peersReplicatedTo.size < numPeersToReplicateTo) {
      val peer = peersForReplication.head
      try {
        val onePeerStartTime = System.nanoTime
        logTrace(s"Trying to replicate $blockId of ${data.size} bytes to $peer")
        // This thread keeps a lock on the block, so we do not want the netty thread to unlock
        // block when it finishes sending the message.
        // 这个线程在块上保持一个锁，所以我们不希望netty线程在块发送完消息后解锁它
        val buffer = new BlockManagerManagedBuffer(blockInfoManager, blockId, data, false,
          unlockOnDeallocate = false)
        blockTransferService.uploadBlockSync(
          peer.host,
          peer.port,
          peer.executorId,
          blockId,
          buffer,
          tLevel,
          classTag)
        logTrace(s"Replicated $blockId of ${data.size} bytes to $peer" +
          s" in ${(System.nanoTime - onePeerStartTime).toDouble / 1e6} ms")
        peersForReplication = peersForReplication.tail
        peersReplicatedTo += peer
      } catch {
        // Rethrow interrupt exception
        case e: InterruptedException =>
          throw e
        // Everything else we may retry
        case NonFatal(e) =>
          logWarning(s"Failed to replicate $blockId to $peer, failure #$numFailures", e)
          peersFailedToReplicateTo += peer
          // we have a failed replication, so we get the list of peers again
          // we don't want peers we have already replicated to and the ones that
          // have failed previously
          val filteredPeers = getPeers(true).filter { p =>
            !peersFailedToReplicateTo.contains(p) && !peersReplicatedTo.contains(p)
          }

          numFailures += 1
          peersForReplication = blockReplicationPolicy.prioritize(
            blockManagerId,
            filteredPeers,
            peersReplicatedTo,
            blockId,
            numPeersToReplicateTo - peersReplicatedTo.size)
      }
    }
    logDebug(s"Replicating $blockId of ${data.size} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took ${(System.nanoTime - startTime) / 1e6} ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
      return false
    }

    logDebug(s"block $blockId replicated to ${peersReplicatedTo.mkString(", ")}")
    return true
  }

  /**
   * Read a block consisting of a single object.
   */
  def getSingle[T: ClassTag](blockId: BlockId): Option[T] = {
    get[T](blockId).map(_.data.next().asInstanceOf[T])
  }

  /**
   * Write a block consisting of a single object.
   *
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
   */
  def putSingle[T: ClassTag](
      blockId: BlockId,
      value: T,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   *
   * 回到BlockManager.scala，看一下dropFromMemory方法。如果存储级别定位为MEMORY_AND_DISK，那么数据可能放在内存和磁盘中，
   * 内存够的情况下不会放到磁盘上；如果内存不够，就放到磁盘上，这时就会调用dropFromMemory。
   * 如果存储级别不是定义为MEMORY_AND_DISK，而只是存储在内存中，内存不够时，缓存的数据此时就会丢弃。
   * 如果仍需要数据，那就要重新计算
   *
   * @return the block's new effective StorageLevel.
   */
  private[storage] override def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {
    logInfo(s"Dropping block $blockId from memory")
    val info = blockInfoManager.assertBlockIsLockedForWriting(blockId)
    var blockIsUpdated = false
    val level = info.level

    // Drop to disk, if storage level requires
    if (level.useDisk && !diskStore.contains(blockId)) {
      logInfo(s"Writing block $blockId to disk")
      data() match {
        case Left(elements) =>
          diskStore.put(blockId) { channel =>
            val out = Channels.newOutputStream(channel)
            serializerManager.dataSerializeStream(
              blockId,
              out,
              elements.toIterator)(info.classTag.asInstanceOf[ClassTag[T]])
          }
        case Right(bytes) =>
          diskStore.putBytes(blockId, bytes)
      }
      blockIsUpdated = true
    }

    // Actually drop from memory store
    val droppedMemorySize =
      if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
    val blockIsRemoved = memoryStore.remove(blockId)
    if (blockIsRemoved) {
      blockIsUpdated = true
    } else {
      logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
    }

    val status = getCurrentBlockStatus(blockId, info)
    if (info.tellMaster) {
      reportBlockStatus(blockId, status, droppedMemorySize)
    }
    if (blockIsUpdated) {
      addUpdatedBlockStatusToTaskMetrics(blockId, status)
    }
    status.storageLevel
  }

  /**
   * Remove all blocks belonging to the given RDD.
   *
   * @return The number of blocks removed.
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfoManager.entries.flatMap(_._1.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  def decommissionBlockManager(): Unit = storageEndpoint.ask(DecommissionBlockManager)

  private[spark] def decommissionSelf(): Unit = synchronized {
    decommissioner match {
      case None =>
        logInfo("Starting block manager decommissioning process...")
        decommissioner = Some(new BlockManagerDecommissioner(conf, this))
        decommissioner.foreach(_.start())
      case Some(_) =>
        logDebug("Block manager already in decommissioning state")
    }
  }

  /**
   *  Returns the last migration time and a boolean denoting if all the blocks have been migrated.
   *  If there are any tasks running since that time the boolean may be incorrect.
   */
  private[spark] def lastMigrationInfo(): (Long, Boolean) = {
    decommissioner.map(_.lastMigrationInfo()).getOrElse((0, false))
  }

  private[storage] def getMigratableRDDBlocks(): Seq[ReplicateBlock] =
    master.getReplicateInfoForRDDBlocks(blockManagerId)

  /**
   * Remove all blocks belonging to the given broadcast.
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfoManager.entries.map(_._1).collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
   * Remove a block from both memory and disk.
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"Removing block $blockId")
    blockInfoManager.lockForWriting(blockId) match {
      case None =>
        // The block has already been removed; do nothing.
        logWarning(s"Asked to remove block $blockId, which does not exist")
      case Some(info) =>
        removeBlockInternal(blockId, tellMaster = tellMaster && info.tellMaster)
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
    }
  }

  /**
   * Internal version of [[removeBlock()]] which assumes that the caller already holds a write
   * lock on the block.
   */
  private def removeBlockInternal(blockId: BlockId, tellMaster: Boolean): Unit = {
    val blockStatus = if (tellMaster) {
      val blockInfo = blockInfoManager.assertBlockIsLockedForWriting(blockId)
      Some(getCurrentBlockStatus(blockId, blockInfo))
    } else None

    // Removals are idempotent in disk store and memory store. At worst, we get a warning.
    val removedFromMemory = memoryStore.remove(blockId)
    val removedFromDisk = diskStore.remove(blockId)
    if (!removedFromMemory && !removedFromDisk) {
      logWarning(s"Block $blockId could not be removed as it was not found on disk or in memory")
    }

    blockInfoManager.removeBlock(blockId)
    if (tellMaster) {
      // Only update storage level from the captured block status before deleting, so that
      // memory size and disk size are being kept for calculating delta.
      reportBlockStatus(blockId, blockStatus.get.copy(storageLevel = StorageLevel.NONE))
    }
  }

  private def addUpdatedBlockStatusToTaskMetrics(blockId: BlockId, status: BlockStatus): Unit = {
    if (conf.get(config.TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES)) {
      Option(TaskContext.get()).foreach { c =>
        c.taskMetrics().incUpdatedBlockStatuses(blockId -> status)
      }
    }
  }

  def releaseLockAndDispose(
      blockId: BlockId,
      data: BlockData,
      taskContext: Option[TaskContext] = None): Unit = {
    releaseLock(blockId, taskContext)
    data.dispose()
  }

  def stop(): Unit = {
    decommissioner.foreach(_.stop())
    blockTransferService.close()
    if (blockStoreClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      blockStoreClient.close()
    }
    remoteBlockTempFileManager.stop()
    diskBlockManager.stop()
    rpcEnv.stop(storageEndpoint)
    blockInfoManager.clear()
    memoryStore.clear()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
  }
}


private[spark] object BlockManager {
  private val ID_GENERATOR = new IdGenerator

  def blockIdsToLocations(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map { loc =>
        ExecutorCacheTaskLocation(loc.host, loc.executorId).toString
      }
    }
    blockManagers.toMap
  }

  private class ShuffleMetricsSource(
      override val sourceName: String,
      metricSet: MetricSet) extends Source {

    override val metricRegistry = new MetricRegistry
    metricRegistry.registerAll(metricSet)
  }

  class RemoteBlockDownloadFileManager(blockManager: BlockManager)
      extends DownloadFileManager with Logging {
    // lazy because SparkEnv is set after this
    lazy val encryptionKey = SparkEnv.get.securityManager.getIOEncryptionKey()

    private class ReferenceWithCleanup(
        file: DownloadFile,
        referenceQueue: JReferenceQueue[DownloadFile]
        ) extends WeakReference[DownloadFile](file, referenceQueue) {

      val filePath = file.path()

      def cleanUp(): Unit = {
        logDebug(s"Clean up file $filePath")

        if (!file.delete()) {
          logDebug(s"Fail to delete file $filePath")
        }
      }
    }

    private val referenceQueue = new JReferenceQueue[DownloadFile]
    private val referenceBuffer = Collections.newSetFromMap[ReferenceWithCleanup](
      new ConcurrentHashMap)

    private val POLL_TIMEOUT = 1000
    @volatile private var stopped = false

    private val cleaningThread = new Thread() { override def run(): Unit = { keepCleaning() } }
    cleaningThread.setDaemon(true)
    cleaningThread.setName("RemoteBlock-temp-file-clean-thread")
    cleaningThread.start()

    override def createTempFile(transportConf: TransportConf): DownloadFile = {
      val file = blockManager.diskBlockManager.createTempLocalBlock()._2
      encryptionKey match {
        case Some(key) =>
          // encryption is enabled, so when we read the decrypted data off the network, we need to
          // encrypt it when writing to disk.  Note that the data may have been encrypted when it
          // was cached on disk on the remote side, but it was already decrypted by now (see
          // EncryptedBlockData).
          new EncryptedDownloadFile(file, key)
        case None =>
          new SimpleDownloadFile(file, transportConf)
      }
    }

    override def registerTempFileToClean(file: DownloadFile): Boolean = {
      referenceBuffer.add(new ReferenceWithCleanup(file, referenceQueue))
    }

    def stop(): Unit = {
      stopped = true
      cleaningThread.interrupt()
      cleaningThread.join()
    }

    private def keepCleaning(): Unit = {
      while (!stopped) {
        try {
          Option(referenceQueue.remove(POLL_TIMEOUT))
            .map(_.asInstanceOf[ReferenceWithCleanup])
            .foreach { ref =>
              referenceBuffer.remove(ref)
              ref.cleanUp()
            }
        } catch {
          case _: InterruptedException =>
            // no-op
          case NonFatal(e) =>
            logError("Error in cleaning thread", e)
        }
      }
    }
  }

  /**
   * A DownloadFile that encrypts data when it is written, and decrypts when it's read.
   */
  private class EncryptedDownloadFile(
      file: File,
      key: Array[Byte]) extends DownloadFile {

    private val env = SparkEnv.get

    override def delete(): Boolean = file.delete()

    override def openForWriting(): DownloadFileWritableChannel = {
      new EncryptedDownloadWritableChannel()
    }

    override def path(): String = file.getAbsolutePath

    private class EncryptedDownloadWritableChannel extends DownloadFileWritableChannel {
      private val countingOutput: CountingWritableChannel = new CountingWritableChannel(
        Channels.newChannel(env.serializerManager.wrapForEncryption(new FileOutputStream(file))))

      override def closeAndRead(): ManagedBuffer = {
        countingOutput.close()
        val size = countingOutput.getCount
        new EncryptedManagedBuffer(new EncryptedBlockData(file, size, env.conf, key))
      }

      override def write(src: ByteBuffer): Int = countingOutput.write(src)

      override def isOpen: Boolean = countingOutput.isOpen()

      override def close(): Unit = countingOutput.close()
    }
  }
}
