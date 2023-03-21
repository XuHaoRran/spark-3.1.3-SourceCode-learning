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

import org.apache.spark.{Partitioner, RangePartitioner}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging

/**
 * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
 * an implicit conversion. They will work with any key type `K` that has an implicit `Ordering[K]`
 * in scope. Ordering objects already exist for all of the standard primitive types. Users can also
 * define their own orderings for custom types, or to override the default ordering. The implicit
 * ordering that is in the closest scope will be used.
 *
 * {{{
 *   import org.apache.spark.SparkContext._
 *
 *   val rdd: RDD[(String, Int)] = ...
 *   implicit val caseInsensitiveOrdering = new Ordering[String] {
 *     override def compare(a: String, b: String) =
 *       a.toLowerCase(Locale.ROOT).compare(b.toLowerCase(Locale.ROOT))
 *   }
 *
 *   // Sort by key, using the above case insensitive ordering.
 *   rdd.sortByKey()
 * }}}
 */
class OrderedRDDFunctions[K : Ordering : ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag] @DeveloperApi() (
    self: RDD[P])
  extends Logging with Serializable {
  private val ordering = implicitly[Ordering[K]]

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   * 当需要对分区内的数据进行排序时，会设置RDD中的宽依赖（ShuffleDependency）实例的keyOrdering变量。
   *
   * 当需要对分区内部的数据进行排序时，构建RDD的同时会设置Key值的排序算法，
   * 结合前面的read代码，当指定Key值的排序算法时，
   * 就会使用外部排序器对分区内的数据进行排序
   * 。
   */
  // TODO: this currently doesn't work on P other than Tuple2!
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      : RDD[(K, V)] = self.withScope
  {
    // 注意，这里设置了该方法构建的RDD 使用的分区器
    // 根据Range而非Hash进行分区，对应的Range信息需要计算并将结果
    // 反馈到Driver端，因此对应调用RDD中的Action，即会触发一个Job的执行
    val part = new RangePartitioner(numPartitions, self, ascending)
    // 在构建RDD实例后，设置Key的排序算法，即Ordering实例
    new ShuffledRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  /**
   * Repartition the RDD according to the given partitioner and, within each resulting partition,
   * sort records by their keys.
   *
   * 该方法依据Partitioner对RDD进行分区，并且在每个结果分区中按key进行排序；
   * 通过对比sortByKey发现，这种方式比先分区，然后在每个分区中进行排序效率高，
   * 这是因为它可以将排序融入Shuffle阶段。
   *
   * repartitionAndSortWithinPartitions是Spark官网推荐的一个算子。官方建议，如果repartition重分区之后，
   * 还要进行排序，建议直接使用repartitionAndSortWithinPartitions算子。因为该算子可以一边进行重分区的Shuffle操作，
   * 一边进行排序。Shuffle与sort两个操作同时进行，
   * 比先Shuffle再sort性能要高。
   *
   * This is more efficient than calling `repartition` and then sorting within each partition
   * because it can push the sorting down into the shuffle machinery.
   */
  def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
    new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
  }

  /**
   * Returns an RDD containing only the elements in the inclusive range `lower` to `upper`.
   * If the RDD has been partitioned using a `RangePartitioner`, then this operation can be
   * performed efficiently by only scanning the partitions that might contain matching elements.
   * Otherwise, a standard `filter` is applied to all partitions.
   */
  def filterByRange(lower: K, upper: K): RDD[P] = self.withScope {

    def inRange(k: K): Boolean = ordering.gteq(k, lower) && ordering.lteq(k, upper)

    val rddToFilter: RDD[P] = self.partitioner match {
      case Some(rp: RangePartitioner[K, V]) =>
        val partitionIndices = (rp.getPartition(lower), rp.getPartition(upper)) match {
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }
        PartitionPruningRDD.create(self, partitionIndices.contains)
      case _ =>
        self
    }
    rddToFilter.filter { case (k, v) => inRange(k) }
  }

}
