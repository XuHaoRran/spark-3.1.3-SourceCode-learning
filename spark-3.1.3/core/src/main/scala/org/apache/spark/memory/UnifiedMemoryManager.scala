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

package org.apache.spark.memory

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Tests._
import org.apache.spark.storage.BlockId

/**
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
 *
 * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
 * configurable through `spark.memory.fraction` (default 0.6). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.6 * 0.5 = 0.3 of the heap space by default.
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.
 *
 * spark.memory.fraction` (默认值是0.6)
 *
 * UnifiedMemoryManager与StaticMemoryManager一样实现了MemoryManager的几个内存分配、释放的接口，对应分配与释放接口的实现，
 * 在StaticMemoryManager中相对比较简单，而在UnifiedMemoryManager中，由于考虑到动态借用的情况，实现相对比较复杂，
 *
 * 最主要的改变是存储和运行的空间可以动态移动。需要注意的是，执行比存储有更大的优先值，当空间不够时，
 * 可以向对方借空间，但前提是对方有足够的空间或者是Execution可以强制把Storage一部分空间挤掉。
 * Excution向Storage借空间有两种方式：第一种方式是Storage曾经向Execution借了空间，它缓存的数据可能非常多，
 * 当Execution需要空间时，可以强制拿回来；第二种方式是Storage Memory不足50%的情况下，
 * Storage Memory会很乐意地把剩余空间借给Execution
 * 。
 * @param onHeapStorageRegionSize Size of the storage region, in bytes.
 *                          This region is not statically reserved; execution can borrow from
 *                          it if necessary. Cached blocks can be evicted only if actual
 *                          storage memory usage exceeds this region.
 */
private[spark] class UnifiedMemoryManager(
    conf: SparkConf,
    val maxHeapMemory: Long, //  maxMemory = (systemMemory - RESERVED_SYSTEM_MEMORY_BYTES(300MB)) * 0.6
    onHeapStorageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize) {

  private def assertInvariants(): Unit = {
    assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
    assert(
      offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
  }

  assertInvariants()

  override def maxOnHeapStorageMemory: Long = synchronized {
    maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  override def maxOffHeapStorageMemory: Long = synchronized {
    maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * 尝试为当前任务获取最多`numBytes`的执行内存，并返回获得的字节数，如果无法分配，则返回0。此调用可能会阻塞，
   * 直到某些情况下有足够的空闲内存，以确保每个任务都有机会在强制溢出之前至少达到总内存池的1 / 2N（其中N是活动任务的#）。
   * 如果任务数量增加，这可能发生，但旧的Task已经分配了内存
   */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
      // 当前内存模式是堆内存，使用onHeapExecutionMemoryPool内存池管理
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        onHeapStorageRegionSize,
        maxHeapMemory)
      // 当前内存模式是堆外内存，使用offHeapExecutionMemoryPool内存池管理

      // 因此在启动Off-Heap内存模式时，可以将Storage的内存占比（对应配置属性spark.memory.storageFraction）设置高一点，
      // 虽然在具体分配过程中，Storage也可以向On-Heap这部分Execution借用内存。
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        offHeapStorageMemory,
        maxOffHeapMemory)
    }

    /**
     * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
     *
     * When acquiring memory for a task, the execution pool may need to make multiple
     * attempts. Each attempt must be able to evict storage in case another task jumps in
     * and caches a large block between the attempts. This is called once per attempt.
     * 增加执行池的大小，通过驱逐缓存的块，从而缩小存储池。当为任务分配内存时，执行池可能需要进行多次尝试。
     * 每一次尝试都必须能够驱逐存储，以防另一个任务在尝试之间跳入并缓存一个大块。这是每次尝试调用一次。
     *
     */
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {
        // There is not enough free memory in the execution pool, so try to reclaim memory from
        // storage. We can reclaim any free memory from the storage pool. If the storage pool
        // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
        // the memory that storage has borrowed from execution.

        // 可以从storagePool中回收的内存大小 = storage中空闲的memory 和 storagePool的大小 - storageRegionSize 中的最大值
        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree,
          storagePool.poolSize - storageRegionSize)
        // 如果可以回收的内存大于0
        if (memoryReclaimableFromStorage > 0) {
          // Only reclaim as much space as is necessary and available:
          // 回收的空间 = 需要的额外内存 和 可以回收的内存 中的最小值
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
            math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
          // 减少storagePool的内存
          storagePool.decrementPoolSize(spaceToReclaim)
          // 增加executionPool的内存
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    /**
     * The size the execution pool would have after evicting storage memory.
     *
     * The execution memory pool divides this quantity among the active tasks evenly to cap
     * the execution memory allocation for each task. It is important to keep this greater
     * than the execution pool size, which doesn't take into account potential memory that
     * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
     *
     * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
     * in execution memory allocation across tasks, Otherwise, a task may occupy more than
     * its fair share of execution memory, mistakenly thinking that other tasks can acquire
     * the portion of storage memory that cannot be evicted.
     *
     * maybeGrowExecutionPool方法首先判断申请的内存大于0，然后判断剩余空间和Storage曾经占用的空间多，
     * 把需要的内存资源量提交给StorageMemoryPool的freeSpaceToShrinkPool方法。
     */
    def computeMaxExecutionPoolSize(): Long = {
      // 最大的executionpoolsize = maxMemory - min(已经使用的storagepoolsize, storageRegionSize)
      maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
    }
    // 最终调用MemoryPool的acquireMemory方法，申请内存
    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, () => computeMaxExecutionPoolSize)
  }

  /**
   * 主要是为当前的执行任务获得的执行空间，它首先会根据onHeap和offHeap方式进行分配
   * @param blockId
   * @param numBytes
   * @param memoryMode
   *  @return whether all N bytes were successfully granted.
   */
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        maxOnHeapStorageMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        maxOffHeapStorageMemory)
    }
    if (numBytes > maxMemory) {
      // Fail fast if the block simply won't fit
      // 如果block的大小超过了最大内存，那么直接返回false
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxMemory bytes)")
      return false
    }
    if (numBytes > storagePool.memoryFree) {
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      // 如果申请的内存大于storagePool的剩余内存，那么就从executionPool中借用内存
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,
        numBytes - storagePool.memoryFree)
      executionPool.decrementPoolSize(memoryBorrowedFromExecution)
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    storagePool.acquireMemory(blockId, numBytes)
  }

  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}

object UnifiedMemoryManager {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
  // 为非存储和非执行目的保留固定数量的内存。提供与spark.memory.fraction类似的功能，但保证即使对于小堆也保留足够的内存。
  // 例如，如果我们有一个1GB的JVM，则执行和存储的内存将为（1024-300）* 0.6 = 434MB。
  //
  // 里面的预留空间是300MB
  //
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    // maxMemory = (systemMemory - RESERVED_SYSTEM_MEMORY_BYTES(300MB)) * 0.6
    val maxMemory = getMaxMemory(conf)
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      // 通过配置属性spark.memory.storageFraction来设置存储内存占用比例，默认为0.5
      // (system内存-预留内存) * 0. 6 * MEMORY_STORAGE_FRACTION(0.5) =  (system内存-预留内存) * 0.3 = 堆内Storage内存空间
      onHeapStorageRegionSize =
        (maxMemory * conf.get(config.MEMORY_STORAGE_FRACTION)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   * 返回执行和存储之间共享的总内存量（以字节为单位）。
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    val systemMemory = conf.get(TEST_MEMORY)
    // 当前系统内存,默认为300MB
    val reservedMemory = conf.getLong(TEST_RESERVED_MEMORY.key,
      if (conf.contains(IS_TESTING)) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    // 当前最小系统内存，需要300×1.5=450MB，不满足该条件就会报错退出
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or ${config.DRIVER_MEMORY.key} in Spark configuration.")
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    // 检查执行器内存，如果内存不足，则快速失败
    // 默认1g
    if (conf.contains(config.EXECUTOR_MEMORY)) {
      val executorMemory = conf.getSizeAsBytes(config.EXECUTOR_MEMORY.key)
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or ${config.EXECUTOR_MEMORY.key} in Spark configuration.")
      }
    }
    // 剩下可用的内存
    val usableMemory = systemMemory - reservedMemory

    // 当前Executor与Storage共享的最大内存，可用内存×0.6=600MB
    // 用户内存可用内存×0.4=400MB

    // 用于执行和存储的部分（堆空间 - 300MB）。该值越小，溢出和缓存数据驱逐发生的频率越高。
    // 该配置的目的是为内部元数据、用户数据结构和稀疏、异常大记录的不精确大小估计预留内存。建议将其保留为默认值。
    val memoryFraction = conf.get(config.MEMORY_FRACTION)
    (usableMemory * memoryFraction).toLong
  }
}
