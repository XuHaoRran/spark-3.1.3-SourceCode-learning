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

package org.apache.spark.sql.catalyst.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * 1.Rule在SparkSQL的Analyzer、Optimizer、SparkPlan等各个组件中都会应用到。
 * 2.Rule是一个抽象类，具体的Rule实现是通过RuleExecutor完成的。
 * 凡需要处理执行计划树（Analyze过程、Optimize过程、SparkStrategy过程），
 * 实施规则匹配和节点处理的，都需要继承RuleExecutor[TreeType]抽象类。
 * RuleExecutor内部提供了一个Seq[Batch]，里面定义的是该RuleExecutor的处理步骤。
 * 每个Batch代表一套规则，配备一个策略，该策略说明了迭代次数（一次，还是多次）。
 * 3.Rule通过定义Once（默认为1）和FixedPoint，可以对Tree进行一次操作或多次操作
 * （如对某些Tree进行多次迭代操作的时候，达到FixedPoint次数迭代或达到前后两次的树结构没变化才停止操作，
 * 具体参看RuleExecutor.apply。
 * @tparam TreeType
 */
abstract class Rule[TreeType <: TreeNode[_]] extends SQLConfHelper with Logging {

  /** Name for this rule, automatically inferred based on class name. */
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(plan: TreeType): TreeType
}
