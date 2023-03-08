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

import scala.collection.mutable.HashSet

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.util.CallSite

/**
 * A stage is a set of parallel tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * other stage(s), or a result stage, in which case its tasks directly compute a Spark action
 * (e.g. count(), save(), etc) by running a function on an RDD. For shuffle map stages, we also
 * track the nodes that each output partition is on.
 *
 * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * Finally, a single stage can be re-executed in multiple attempts due to fault recovery. In that
 * case, the Stage object will track multiple StageInfo objects to pass to listeners or the web UI.
 * The latest one will be accessible through latestInfo.
 *
 * <p>这样，Stage对象可以跟踪多个StageInfo（存储SparkListeners监听到的Stage的信息，将Stage信息传递给Listeners或web UI）。
 * 默认重试次数为4次，且可以直接运行计算失败的阶段，只计算失败的数据分片，Stage的源码如下所示。
 *
 * <p>Stage是Spark Job运行时具有相同逻辑功能和并行计算任务的一个基本单元。Stage中所有的任务都依赖同样的Shuffle，
 * 每个DAG任务通过DAGScheduler在Stage的边界处发生Shuffle形成Stage，然后DAGScheduler运行这些阶段的拓扑顺序。
 * 每个Stage都可能是ShuffleMapStage，如果是ShuffleMapStage，则跟踪每个输出节点（nodes）上的输出文件分区，
 * 它的任务结果是输入其他的Stage(s)，或者输入一个ResultStage，若输入一个ResultStage，
 * 这个ResultStage的任务直接在这个RDD上运行计算这个Spark Action的函数（如count()、save()等），
 * 并生成shuffleDep等字段描述Stage和生成变量，如outputLocs和numAvailableOutputs，为跟踪map输出做准备。
 * 每个Stage会有firstjobid，确定第一个提交Stage的Job，使用FIFO调度时，会使得其前面的Job先行计算或快速恢复（失败时）。
 * @param id Unique stage ID
 * @param rdd RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
 *   on, while for a result stage, it's the target RDD that we ran an action on
 * @param numTasks Total number of tasks in stage; result stages in particular may not need to
 *   compute all partitions, e.g. for first(), lookup(), and take().
 * @param parents List of stages that this stage depends on (through shuffle dependencies).
 * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
 * @param callSite Location in the user program associated with this stage: either where the target
 *   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
 *

 */
private[scheduler] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val parents: List[Stage],
    val firstJobId: Int,
    val callSite: CallSite,
    val resourceProfileId: Int)
  extends Logging {

  val numPartitions = rdd.partitions.length

  /** Set of jobs that this stage belongs to.
   * 属于这个工作集的stage
   * */
  val jobIds = new HashSet[Int]

  /** The ID to use for the next new attempt for this stage.
   * 用于此stage的下一个新的attempt的标识ID
   * */
  private var nextAttemptId: Int = 0

  val name: String = callSite.shortForm
  val details: String = callSite.longForm

  /**
   * Pointer to the [[StageInfo]] object for the most recent attempt. This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
   *
   * <p>最新的[stageInfo] object指针，需要被初始化，任何attempts都是被创造出来的，因为DAGScheduler使用StageInfo告诉SparkListeners
   * 工作何时开始（即发生的任何阶段已经创建）
   */
  private var _latestInfo: StageInfo =
    StageInfo.fromStage(this, nextAttemptId, resourceProfileId = resourceProfileId)

  /**
   * Set of stage attempt IDs that have failed. We keep track of these failures in order to avoid
   * endless retries if a stage keeps failing.
   * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
   * multiple tasks from the same stage attempt fail (SPARK-5945).
   *
   * <p>设置stage attempt IDs，当失败时可以读取失败信息，跟踪这些失败，为了避免无休止的重复失败
   * 跟踪每一次attempt，以便避免记录重复故障，如果从同一个stage创建多任务失败
   */
  val failedAttemptIds = new HashSet[Int]

  private[scheduler] def clearFailures() : Unit = {
    failedAttemptIds.clear()
  }

  /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID.
   * 通过使用新ID创建新的StageInfo，为此阶段创建新尝试
   * */
  def makeNewStageAttempt(
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    val metrics = new TaskMetrics
    metrics.register(rdd.sparkContext)
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences,
      resourceProfileId = resourceProfileId)
    nextAttemptId += 1
  }

  /** Returns the StageInfo for the most recent attempt for this stage.
   * 返回此阶段最近尝试的StageInfo
   * */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed).
   * 返回即将计算的分区ID序列（即需要计算）
   * */
  def findMissingPartitions(): Seq[Int]

  def isIndeterminate: Boolean = {
    rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE
  }
}
