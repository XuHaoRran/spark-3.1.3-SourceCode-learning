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

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus

/**
 * The interface for customizing shuffle write process. The driver create a ShuffleWriteProcessor
 * and put it into [[ShuffleDependency]], and executors use it in each ShuffleMapTask.
 */
private[spark] class ShuffleWriteProcessor extends Serializable with Logging {

  /**
   * Create a [[ShuffleWriteMetricsReporter]] from the task context. As the reporter is a
   * per-row operator, here need a careful consideration on performance.
   */
  protected def createMetricsReporter(context: TaskContext): ShuffleWriteMetricsReporter = {
    context.taskMetrics().shuffleWriteMetrics
  }

  /**
   * The write process for particular partition, it controls the life circle of [[ShuffleWriter]]
   * get from [[ShuffleManager]] and triggers rdd compute, finally return the [[MapStatus]] for
   * this task.
   */
  def write(
      rdd: RDD[_],
      dep: ShuffleDependency[_, _, _],
      mapId: Long,
      context: TaskContext,
      partition: Partition): MapStatus = {
    var writer: ShuffleWriter[Any, Any] = null
    try {
      // 实例化shuffleManager
      val manager = SparkEnv.get.shuffleManager
      // 然后从shuffleDependency中获取注册到ShuffleManager时得到的shufflehandle
      // 根据shufflehandle和当前Task对应的分区ID，获取ShuffleWriter
      writer = manager.getWriter[Any, Any](
        dep.shuffleHandle,
        mapId,
        context,
        createMetricsReporter(context))
      // 将计算结果通过writer对象的write方法写出
      writer.write(
        // 会调用RDD的iterator，然后针对partition进行计算
        rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
}
