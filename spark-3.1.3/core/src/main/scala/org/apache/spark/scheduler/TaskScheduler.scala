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

import scala.collection.mutable.Map

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

/**
 * Low-level task scheduler interface, currently implemented exclusively by
 * [[org.apache.spark.scheduler.TaskSchedulerImpl]].
 * This interface allows plugging in different task schedulers. Each TaskScheduler schedules tasks
 * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
 * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
 * them, retrying if there are failures, and mitigating stragglers. They return events to the
 * DAGScheduler.
 */
private[spark] trait TaskScheduler {

  private val appId = "spark-application-" + System.currentTimeMillis

  def rootPool: Pool

  def schedulingMode: SchedulingMode

  def start(): Unit

  // Invoked after system has successfully initialized (typically in spark context).
  // Yarn uses this to bootstrap allocation of resources based on preferred locations,
  // wait for executor registrations, etc.

  // 成功初始化后调用(通常在 Spark 上下文中)。Yarn 使用这个来引导基于优先位置的资源
  // 分配，等待从节点登记等
  def postStartHook(): Unit = { }

  // Disconnect from the cluster.
  // 从群集断开连接
  def stop(): Unit

  // Submit a sequence of tasks to run.
  // 提交要运行的任务序列
  def submitTasks(taskSet: TaskSet): Unit

  // Kill all the tasks in a stage and fail the stage and all the jobs that depend on the stage.
  // Throw UnsupportedOperationException if the backend doesn't support kill tasks.
  // 取消stage
  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

  /**
   * Kills a task attempt.
   * Throw UnsupportedOperationException if the backend doesn't support kill a task.
   *
   * 杀掉尝试任务
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean

  // Kill all the running task attempts in a stage.
  // Throw UnsupportedOperationException if the backend doesn't support kill tasks.
  def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit

  // Notify the corresponding `TaskSetManager`s of the stage, that a partition has already completed
  // and they can skip running tasks for it.
  def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit

  // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
  // 系统为upcalss设置DAG调度，这是保证在submitTasks被调用前被设置
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  // 获取集群中使用的默认并行级别，作为对作业的提示
  def defaultParallelism(): Int

  /**
   * Update metrics for in-progress tasks and executor metrics, and let the master know that the
   * BlockManager is still alive. Return true if the driver knows about the given block manager.
   * Otherwise, return false, indicating that the block manager should re-register.
   *
   * 更新正运行任务，让master 知道 BlockManager 仍活着。如果driver 知道给定的块管理器，
   * 则返回true;否则返回false，指示块管理器应重新注册
   */
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: Map[(Int, Int), ExecutorMetrics]): Boolean

  /**
   * Get an application ID associated with the job.
   * 获取与作业相关联的应用程序ID
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Process a decommissioning executor.
   */
  def executorDecommission(executorId: String, decommissionInfo: ExecutorDecommissionInfo): Unit

  /**
   * If an executor is decommissioned, return its corresponding decommission info
   */
  def getExecutorDecommissionState(executorId: String): Option[ExecutorDecommissionState]

  /**
   * Process a lost executor
   * 处理丢失的executor
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * Process a removed worker
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit

  /**
   * Get an application's attempt ID associated with the job.
   * 获取与作业相关的应用程序的尝试ID
   * @return An application's Attempt ID
   */
  def applicationAttemptId(): Option[String]

}
