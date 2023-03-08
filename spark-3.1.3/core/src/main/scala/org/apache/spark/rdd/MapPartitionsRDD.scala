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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 *
 * @param prev the parent RDD.
 * @param f The function used to map a tuple of (TaskContext, partition index, input iterator) to
 *          an output iterator.
 * @param preservesPartitioning Whether the input function preserves the partitioner, which should
 *                              be `false` unless `prev` is a pair RDD and the input function
 *                              doesn't modify the keys.
 * @param isFromBarrier Indicates whether this RDD is transformed from an RDDBarrier, a stage
 *                      containing at least one RDDBarrier shall be turned into a barrier stage.
 * @param isOrderSensitive whether or not the function is order-sensitive. If it's order
 *                         sensitive, it may return totally different result when the input order
 *                         is changed. Mostly stateful functions are order-sensitive.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
   // 此RDD是否从RDDBarrier转换，至少含有一个RDDBarrier的Stage阶段将转变为屏障阶段
    isFromBarrier: Boolean = false,
   // 指示函数是否区分顺序
    isOrderSensitive: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  @transient protected lazy override val isBarrier_ : Boolean =
    isFromBarrier || dependencies.exists(_.rdd.isBarrier())

  /**
   * 获取DeterministicLevel的Value值
   * 其中DeterministicLevel定义了RDD输出结果的确定级别（即“RDD compute”返回的值）。当Spark RDD重新运行任务时，输出将有所不同。
   * （1）确定DETERMINATE：在重新运行后，RDD输出总是以相同顺序的相同数据集。
   * （2）无序：RDD输出总是相同的数据集，但重新运行之后顺序可能不同。
   * （3）不确定的。重新运行后，RDD输出可能不同。
   *  注意，RDD的输出通常依赖于父RDD。当父RDD的输出是不确定的，很可能RDD的输出也是不确定的。
   *
   *
   *  <p>例如，在计算的过程中，会产生很多的数据碎片，这时产生一个Partition可能会非常小，如果一个Partition非常小，
   *  每次都会消耗一个线程去处理，这时可能会降低它的处理效率，需要考虑把许多小的Partition合并成一个较大的Partition去处理，
   *  这样会提高效率。另外，有可能内存不是那么多，而每个Partition的数据Block比较大，这时需要考虑把Partition变成更小的数据分片，这样让Spark处理更多的批次，
   *  但是不会出现OOM。
   * @return
   */
  override protected def getOutputDeterministicLevel = {
    if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
  }
}
