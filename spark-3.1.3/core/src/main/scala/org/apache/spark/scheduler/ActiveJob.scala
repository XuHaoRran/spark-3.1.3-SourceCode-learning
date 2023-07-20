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

package org.apache.spark.scheduler

import java.util.Properties

import org.apache.spark.util.CallSite

/**
 * A running job in the DAGScheduler. Jobs can be of two types: a result job, which computes a
 * ResultStage to execute an action, or a map-stage job, which computes the map outputs for a
 * ShuffleMapStage before any downstream stages are submitted. The latter is used for adaptive
 * query planning, to look at map output statistics before submitting later stages. We distinguish
 * between these two types of jobs using the finalStage field of this class.
 *
 * Jobs are only tracked for "leaf" stages that clients directly submitted, through DAGScheduler's
 * submitJob or submitMapStage methods. However, either type of job may cause the execution of
 * other earlier stages (for RDDs in the DAG it depends on), and multiple jobs may share some of
 * these previous stages. These dependencies are managed inside DAGScheduler.
 *
 * DAGScheduler 中正在运行的作业。
 * 作业可以有两种类型：结果作业（计算结果阶段以执行操作）或地图阶段作业（在提交任何下游阶段之前计算 ShuffleMap 阶段的地图输出）。
 * 后者用于自适应查询规划，在提交后续阶段之前查看地图输出统计数据。我们使用该类的 finalStage 字段来区分这两类作业。
 * 我们只跟踪客户通过 DAGScheduler 的 submitJob 或 submitMapStage 方法直接提交的 "叶子 "阶段的作业。
 * 不过，无论哪种类型的作业，都可能会导致其他早期阶段（对于它所依赖的 DAG 中的 RDD）的执行，
 * 而且多个作业可能会共享其中一些早期阶段。这些依赖关系由 DAGScheduler 管理。
 *
 * <p>保存了当前Job的一些信息
 *
 * @param jobId A unique ID for this job.
 * @param finalStage The stage that this job computes (either a ResultStage for an action or a
 *   ShuffleMapStage for submitMapStage).
 * @param callSite Where this job was initiated in the user's program (shown on UI).
 * @param listener A listener to notify if tasks in this job finish or the job fails.
 * @param properties Scheduling properties attached to the job, such as fair scheduler pool name.
 */
private[spark] class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,
    val callSite: CallSite,
    val listener: JobListener,
    val properties: Properties) {

  /**
   * Number of partitions we need to compute for this job. Note that result stages may not need
   * to compute all partitions in their target RDD, for actions like first() and lookup().
   */
  val numPartitions = finalStage match {
    case r: ResultStage => r.partitions.length
    case m: ShuffleMapStage => m.rdd.partitions.length
  }

  /** Which partitions of the stage have finished */
  val finished = Array.fill[Boolean](numPartitions)(false)

  var numFinished = 0

  /** Resets the status of all partitions in this stage so they are marked as not finished. */
  def resetAllPartitions(): Unit = {
    (0 until numPartitions).foreach(finished.update(_, false))
    numFinished = 0
  }
}
