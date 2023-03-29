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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    shuffleExecutorComponents: ShuffleExecutorComponents)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /** Write a bunch of records to this task's output
   *  然后在计算具体的Partition之后，通过shuffleManager获得的shuffleWriter把当前Task计算的结果根据具体的shuffleManager实现写入到
   *  具体的文件中，操作完成后会把MapStatus发送给Driver端的DAGScheduler的MapOutputTracker。
   *
   *  1.在insertAll之前有判断，在Mapper端是否要进行聚合，如果没有进行聚合，
   *  将按照Partition写入到不同的文件中，最后按照Partition顺序合并到同样一个文件中。
   *  在这种情况下，适合Partition的数据比较少的情况；那我们将很多的bucket合并到一个文件，
   *  减少了Mapper端输出文件的数量，减少了磁盘I/O，提升了性能。
   *
   *  2.除了既不想排序，又不想聚合的情况，也可能在Mapper端不进行聚合，但可能进行排序，
   *  这在缓存区中根据PartitionID进行排序，也可能根据Key进行排序。最后需要根据PartitionID进行排序，
   *  比较适合Partition比较多的情况。如果内存不够用，就会溢写到磁盘中
   *  。
   *  。
   *
   *  3.第三种情况，既需要聚合，也需要排序，这时肯定先进行聚合，后进行排序。
   *  实现时，根据Key值进行聚合，在缓存中根据PartitionID进行排序，也可能根据Key进行排序，
   *  默认情况不需要根据Key进行排序。最后需要根据PartitionID进行合并，如果内存不够用，
   *  就会溢写到磁盘中。
   * */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // 当需要在Map端进行聚合操作时，此时将会指定聚合器(Aggregator)
    // 将key值的Ordering传入到外部排序器ExternalSorter中
    sorter = if (dep.mapSideCombine) {
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      // 没有指定Map端使用聚合时，传入ExternalSorter的聚合器与key值的ordering都设为None，即不需要传入，对应在Reduce端读取数据时
      // 才根据聚合器分区数据进行聚合，并根据是否设置Ordering而选择是否对分区数据进行排序
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    // 将写入的记录的记录集全部放入外部排序器
    // 基于sorter具体排序的实现方式，将数据写入缓冲区中。如果records数据特别多，可能会导致内存溢出，Spark现在的实现方式是Spill溢出写到磁盘中
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    // 不要费心在Shuffle写时间中，包括打开合并输出文件的时间，因为它只打开一个文件，所以通常太快，无法精确测量
    // 和BypassMergeSortShuffleWriter一样，获取输出文件名和BlockId
    val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(
      dep.shuffleId, mapId, dep.partitioner.numPartitions)
    sorter.writePartitionedMapOutput(dep.shuffleId, mapId, mapOutputWriter)
    // 将分区数据写入文件，返回各个分区对应的数据量
    val partitionLengths = mapOutputWriter.commitAllPartitions().getPartitionLengths
    // map在最后地时候我们将元数据写入到MapStatus，MapStatus返回给Driver，
    // 这里是元数据信息，Driver根据这个信息告诉下一个Stage，你的上一个Mapper的数据写在什么地方。下一个Stage就根据MapStatus得到上一个Stage的处理结果。
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    // 如果需要在Map端进行聚合操作，那么就不能跳过排序
    if (dep.mapSideCombine) {
      false
    } else {
      val bypassMergeThreshold: Int = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
