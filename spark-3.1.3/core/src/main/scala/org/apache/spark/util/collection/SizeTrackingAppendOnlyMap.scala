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

package org.apache.spark.util.collection

/**
 * An append-only map that keeps track of its estimated size in bytes.
 */
private[spark] class SizeTrackingAppendOnlyMap[K, V]
  extends AppendOnlyMap[K, V] with SizeTracker
{
  override def update(key: K, value: V): Unit = {
    super.update(key, value)
    super.afterUpdate()
  }

  // super.changeValue():逻辑就是SizeTrackingAppendOnlyMap中有一个数组data，2*i存放key，
  // 2*i+1存放value，每次过来一个key时先将key进行rehash计算出在数组data中的初始位置，
  // 然后逐步+1的进行遍历，如果找到data中存在的相同的key值，就进行数据聚合更新，如果data中没有key，
  // 说明是一个新key，添加到数组中，并判断data是否需要扩容。注意这里的key是(pid,key),
  // 不过这并不影响，因为key相同的话pid也是相同的
  override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    val newValue = super.changeValue(key, updateFunc)
    // 每次更新后调用回调，在其中会调用takeSample方法，取一个新样本的当前集合的大小，其中采用estimate方法进行估值

    //当被混入的集合的每次update操作以后，需要执行SizeTracker的afterUpdate方法，afterUpdate会判断这是第几次更新，
    // 需要的话就会使用SizeEstimator的estimate方法来估计下集合的大小。由于SizeEstimator的调用开销比较大，注释上说会是数毫秒，
    // 所以不能频繁调用。所以SizeTracker会记录更新的次数，发生estimate的次数是指数级增长的，基数是1.1，
    // 所以调用estimate时更新的次数会是1.1, 1.1 * 1.1, 1.1 * 1.1 *1.1, ....
    //
    //这是指数的初始增长是很慢的， 1.1的96次方会是1w, 1.1 ^ 144次方是100w，
    // 即对于1w次update，它会执行96次estimate，对10w次update执行120次estimate, 对100w次update执行144次estimate，对1000w次update执行169次。
    super.afterUpdate()
    newValue
  }

  override protected def growTable(): Unit = {
    super.growTable()
    resetSamples()
  }
}
