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

import org.apache.spark.Partition

/**
 * Enumeration to manage state transitions of an RDD through checkpointing
 *
 * [ Initialized --{@literal >} checkpointing in progress --{@literal >} checkpointed ]
 */
private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value
  val Initialized, CheckpointingInProgress, Checkpointed = Value
}

/**
 * This class contains all the information related to RDD checkpointing. Each instance of this
 * class is associated with an RDD. It manages process of checkpointing of the associated RDD,
 * as well as, manages the post-checkpoint state by providing the updated partitions,
 * iterator and preferred locations of the checkpointed RDD.
 *
 * RDDCheckpointData标识某个RDD要进行checkpoint。如果某个RDD要进行checkpoint，那在Spark框架内部就会生成RDDCheckpointData
 */
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends Serializable {

  import CheckpointState._

  // The checkpoint state of the associated RDD.
  // 相关的RDD检查状态
  protected var cpState = Initialized

  // The RDD that contains our checkpointed data
  // RDD包含检查点数据
  private var cpRDD: Option[CheckpointRDD[T]] = None

  // TODO: are we sure we need to use a global lock in the following methods?

  /**
   *
   * Return whether the checkpoint data for this RDD is already persisted.
   *
   * 返回RDD的checkpoint数据是否已经持久化
   */
  def isCheckpointed: Boolean = RDDCheckpointData.synchronized {
    cpState == Checkpointed
  }

  /**
   * Materialize this RDD and persist its content.
   * This is called immediately after the first action invoked on this RDD has completed.
   *
   * 物化RDD和持久化其内容
   * RDD的第一个行动完成以后立即触发调用
   *
   * 实际上在生产时会调用ReliableRDDCheckpointData的doCheckpoint，
   * 在生产过程中会导致ReliableCheckpointRDD的writeRDDToCheckpointDirectory的调用
   */
  final def checkpoint(): Unit = {
    // Guard against multiple threads checkpointing the same RDD by
    // atomically flipping the state of this RDDCheckpointData
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }

    val newRDD = doCheckpoint()

    // Update our state and truncate the RDD lineage
    // 更新我们的状态和截断RDD的血统
    RDDCheckpointData.synchronized {
      cpRDD = Some(newRDD)
      cpState = Checkpointed
      rdd.markCheckpointed()
    }
  }

  /**
   * Materialize this RDD and persist its content.
   *
   * Subclasses should override this method to define custom checkpointing behavior.
   *
   * 物化RDD和持久化其内容
   *
   * 子类应该重写此方法，以定义自定义检查点行为
   * @return the checkpoint RDD created in the process. RDD在进程中创建
   */
  protected def doCheckpoint(): CheckpointRDD[T]

  /**
   *
   * Return the RDD that contains our checkpointed data.
   * This is only defined if the checkpoint state is `Checkpointed`.
   *
   * 返回包含我们检查点数据。如果checkpoint的状态是checkpointed，才定义
   */
  def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized { cpRDD }

  /**
   * Return the partitions of the resulting checkpoint RDD.
   * For tests only.
   *
   * 返回checkpoint RDD的分区，仅用于测试
   */
  def getPartitions: Array[Partition] = RDDCheckpointData.synchronized {
    cpRDD.map(_.partitions).getOrElse { Array.empty }
  }

}

/**
 * Global lock for synchronizing checkpoint operations.
 *
 * 同步检查点操作的全局锁
 */
private[spark] object RDDCheckpointData
