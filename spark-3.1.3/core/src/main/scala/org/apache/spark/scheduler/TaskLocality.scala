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

import org.apache.spark.annotation.DeveloperApi

/**
 * Spark中的数据本地性有以下5种。
 * -PROCESS_LOCAL：进程本地化。代码和数据在同一个进程中，也就是在同一个Executor中；
 *  计算数据的Task由Executor执行，数据在Executor的BlockManager中；性能最好。
 * -NODE_LOCAL：节点本地化。代码和数据在同一个节点中，数据作为一个HDFS block块，
 *  就在节点上，而Task在节点上某个Executor中运行；或者是数据和Task在一个节点上的不同Executor中；
 *  数据需要在进程间进行传输。也就是说，数据虽然在同一Worker中，但不是同一JVM中。这隐含着进程间移动数据的开销。
 * -NO_PREF：数据没有局部性首选位置。它能从任何位置同等访问。对于Task来说，数据从哪里获取都一样，无好坏之分。
 * -RACK_LOCAL：机架本地化。数据在不同的服务器上，但在相同的机架。数据需要通过网络在节点之间进行传输。
 * -ANY：数据在不同的服务器及机架上面。这种方式性能最差。
 *
 * Spark应用程序本身包含代码和数据两部分，单机版本一般情况下很少考虑数据本地性的问题，因为数据在本地。
 * 单机版本的程序，数据本性有PROCESS_LOCAL和NODE_LOCAL之分，但也应尽量让数据处于PROCESS_LOCAL级别。
 *
 * 本地性级别以最近的级别开始，以最远的级别结束。Spark调度任务执行是要让任务获得最近的本地性级别的数据。
 * 然而，有时它不得不放弃最近本地性级别，因为一些资源上的原因，有时不得不选择更远的。在数据与Executor在同一机器但数据处理比较忙，
 * 且存在一些空闲的Executor远离数据的情况下，Spark会让Executor等特定的时间，看其能否完成正在处理的工作。如果该Executor仍不可用，
 * 一个新的任务将启用在更远的可用节点上，即数据被传送给那个节点。
 *
 * 观察大部分Task的数据本地化级别：如果大多都是PROCESS_LOCAL，那就不用调节了；
 * 如果发现很多的级别都是NODE_LOCAL、ANY，那么最好调节一下数据本地化的等待时长。
 * 调节往往不可能一次到位，应该反复调节，每次调节完以后，再运行，观察日志。看看大部分的Task的本地化级别有没有提升；
 * 也看看整个Spark作业的运行时间有没有缩短，但别本末倒置，如果本地化级别提升了，但是因为大量的等待时长而导致Spark作业的运行时间反而增加，
 * 那也是不好的调节。
 */
@DeveloperApi
object TaskLocality extends Enumeration {
  // Process local is expected to be used ONLY within TaskSetManager for now.
  val PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY = Value

  type TaskLocality = Value

  def isAllowed(constraint: TaskLocality, condition: TaskLocality): Boolean = {
    condition <= constraint
  }
}
