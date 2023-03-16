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

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE:
 * 1. This will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 * 2. This contains a method ShuffleBlockResolver which interacts with External Shuffle Service
 * when it is enabled. Need to pay attention to that, if implementing a custom ShuffleManager, to
 * make sure the custom ShuffleManager could co-exist with External Shuffle Service.
 *
 * <p>ShuffleManager是Spark Shuffle系统提供的一个可插拔式接口，可以通过spark.shuffle.manager配置属性来设置自定义的ShuffleManager
 *
 * <p>在Driver和每个Executor的SparkEnv实例化过程中，都会创建一个ShuffleManager，
 * 用于管理块数据，提供集群块数据的读写，包括数据的本地读写和读取远程节点的块数据
 *
 * <p>Shuffle系统的框架可以以ShuffleManager为入口进行解析。在ShuffleManager中指定了整个Shuffle框架使用的各个组件，
 * 包括如何注册到ShuffleManager，以获取一个用于数据读写的处理句柄ShuffleHandle，通过ShuffleHandle获取特定的数据读写接口：
 * ShuffleWriter与ShuffleReader，以及如何获取块数据信息的解析接口ShuffleBlockResolver。
 */
private[spark] trait ShuffleManager {

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   * registerShuffle：每个RDD在构建它的父依赖（这里特指ShuffleDependency）时，
   * 都会先注册到ShuffleManager，获取ShuffleHandler，用于后续数据块的读写等
   *
   * 在Driver端向ShuffleManager注册一个Shuffle，获取一个Handle在具体Tasks中会通过该Handle来读写数据
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** Get a writer for a given partition. Called on executors by map tasks.
   * 可以通过ShuffleHandler获取数据块写入器，写数据时通过Shuffle的块解析器shuffleBlockResolver，
   * 获取写入位置（通常将写入位置抽象为Bucket，位置的选择则由洗牌的规则，即Shuffle的分区器决定），
   * 然后将数据写入到相应位置（理论上，位置可以位于任何能存储数据的地方，包括磁盘、内存或其他存储框架等，
   * 目前在可插拔框架的几种实现中，Spark与Hadoop一样都采用磁盘的方式进行存储，主要目的是为了节约内存，同时提高容错性）
   *
   * 获取对应给定的分区使用的ShuffleWriter，该方法在 Executors 上执行各个Map任务时调用
   * */
  def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V]


  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive) to
   * read from all map outputs of the shuffle.
   *
   * Called on executors by reduce tasks.
   *
   * 可以通过ShuffleHandler获取数据块读取器，然后通过Shuffle的块解析器shuffleBlockResolver，获取指定数据块。
   *
   * 获取在 Reduce 阶段读取分区的 ShuffleReader，对应读取的分区由[startPartitiortoendPartition-1]区间指定。
   * 该方法在Executors上执行，在各个Reduce任务时调用
   */
  final def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    getReader(handle, 0, Int.MaxValue, startPartition, endPartition, context, metrics)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive) to
   * read from a range of map outputs(startMapIndex to endMapIndex-1, inclusive).
   * If endMapIndex=Int.MaxValue, the actual endMapIndex will be changed to the length of total map
   * outputs of the shuffle in `getMapSizesByExecutorId`.
   *
   * Called on executors by reduce tasks.
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   *与注册对应，用于删除元数据等后续清理操作。
   *
   *
   * 该接口和registerShuffle分别负责元数据的取消注册与注册调用unregisterShuffle接口时，会移除ShuffleManager中对应的元数据信息
   * @return true if the metadata removed successfully, otherwise false.
   *
   *
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   *
   * Shuffle的块解析器，通过该解析器，为数据块的读写提供支撑层，便于抽象具体的实现细节
   *
   * 返回一个可以基于块坐标来获取Shuffle 块数据的ShuffleBlockResolver
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit
}
