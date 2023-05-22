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

package org.apache.spark.shuffle

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Fetches and reads the blocks from a shuffle by requesting them from other nodes' block stores.
 *
 * 通过从其他节点上请求读取 Shuffle 数据来接收并读取指定范围[起始分区。结束分区)一对应为左闭右开区间
 * 通过从其他节点上请求读取 Shuffle 数据来接收并读取指定范围[起始分区，结束分区]对应为左闭右开区间
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter, // 每条记录被读取后，要更新Metrics，这样在Web UI控制台或者交互式控制台上就会看到相关的信息。Shuffle的运行肯定需要相关的Metrics。metricIter是一个迭代器。
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    shouldBatchFetch: Boolean = false)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  private def fetchContinuousBlocksInBatch: Boolean = {
    val conf = SparkEnv.get.conf
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(CompressionCodec.createCodec(conf))
    } else {
      true
    }
    val useOldFetchProtocol = conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)
    // SPARK-34790: Fetching continuous blocks in batch is incompatible with io encryption.
    val ioEncryption = conf.get(config.IO_ENCRYPTION_ENABLED)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol && !ioEncryption
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug("The feature tag of continuous shuffle block fetching is set to true, but " +
        "we can not enable the feature because other conditions are not satisfied. " +
        s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
        s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
        s"$useOldFetchProtocol, io encryption: $ioEncryption.")
    }
    doBatchFetch
  }

  /** Read the combined key-values for this reduce task
   * 为该Readuce任务合并key-values值
   * */
  override def read(): Iterator[Product2[K, C]] = {
    // 真正的数据Iterator读取是通过ShuffleBlockFetcherIterator来完成的
    //
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      blocksByAddress,
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      // 传输中的最大大小，调整这个值即表示提高增大reduce缓冲区，减少拉取次数
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      readMetrics,
      fetchContinuousBlocksInBatch).toCompletionIterator

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    // 为每个记录更新上下文任务度量
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    // 为了支持任务取消，这里必须使用可中断迭代器
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)
    // aggregatedIter判断在Mapper端进行聚合怎么做；不在Mapper端聚合怎么做。首先判断aggregator是否被定义，
    // 如果已经定义aggregator，再判断map端是否需聚合，我们谈的是Reducer端，为什么这里需在Mapper端进行聚合呢？
    // 原因很简单：Reducer可能还有下一个Stage，如果还有下一个Stage，那这个Reducer对于下一个Stage而言，
    // 其实就是Mapper，是Mapper就须考虑在本地是否进行聚合。迭代是一个DAG图，假设如果有100个Stage，
    // 这里是第10个Stage，作为第9个Stage的Reducer端，但是作为第11个Stage是Mapper端，作为Shuffle而言，
    // 现在的Reducer端相对于Mapper端。Mapper端需要聚合，则进行combineCombinersByKey。Mapper端也可能不需要聚合，
    // 只需要进行Reducer端的操作。如果aggregator.isDefined没定义，则出错提示。
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        /// 如果在map端已经做了聚合的优化操作，则对读取道德聚合结果进行聚合
        // 注意此时聚合操作与数据类型和map端未做优化时是不同的
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        // map端个分区针对key进行和并后的结果再次聚合
        // map的合并可以大大减少网络传输的数据量
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        // 无需关心值的类型，但确保聚合是兼容的，其将把值的类型转化成聚合以后的C类型
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }
    // 在基于Sort的Shuffle 实现过程中，默认基于PartitionId进行排序
    // 在分区的内部，数据是没有排序的，因此添加了 keyOrdering变量，提供
    // 是否需要针对分区内的数据进行排序的标识信息
    //如果定义了排序，则对输出结果进行排序
    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        // 创建一个外部排序器来对数据进行排序
        // 为了减少内存的压力，避免GC 开销，引入了外部排序器对数据进行排序，当内存不足
        // 以容纳排序的数据量时，会根据配置的spark.shufflespill属性来决定是否需要
        // 溢出到磁盘中，默认情况下会打开spill开关，若不打开spill开关，数据量比
        // 较大时会引发内存溢出问题(OutofMemory，00M)

        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        context.addTaskCompletionListener[Unit](_ => {
          sorter.stop()
        })
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        // 不需要排序分区内部数据时直接返回
        aggregatedIter
    }
    // 如果任务已完成或取消，使用回调停止排序
    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        // 这里使用另一个可终端迭代器来支持任务取消，因为聚合器排序器可能已经使用了以前的可中断迭代器
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}
