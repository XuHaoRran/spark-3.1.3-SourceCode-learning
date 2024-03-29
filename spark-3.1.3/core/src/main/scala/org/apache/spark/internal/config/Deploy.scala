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

package org.apache.spark.internal.config

private[spark] object Deploy {
  val RECOVERY_MODE = ConfigBuilder("spark.deploy.recoveryMode")
    .version("0.8.1")
    .stringConf
    .createWithDefault("NONE")

  val RECOVERY_MODE_FACTORY = ConfigBuilder("spark.deploy.recoveryMode.factory")
    .version("1.2.0")
    .stringConf
    .createWithDefault("")

  val RECOVERY_DIRECTORY = ConfigBuilder("spark.deploy.recoveryDirectory")
    .version("0.8.1")
    .stringConf
    .createWithDefault("")

  val ZOOKEEPER_URL = ConfigBuilder("spark.deploy.zookeeper.url")
    .doc(s"When `${RECOVERY_MODE.key}` is set to ZOOKEEPER, this " +
      "configuration is used to set the zookeeper URL to connect to.")
    .version("0.8.1")
    .stringConf
    .createOptional

  val ZOOKEEPER_DIRECTORY = ConfigBuilder("spark.deploy.zookeeper.dir")
    .version("0.8.1")
    .stringConf
    .createOptional

  val RETAINED_APPLICATIONS = ConfigBuilder("spark.deploy.retainedApplications")
    .version("0.8.0")
    .intConf
    .createWithDefault(200)

  val RETAINED_DRIVERS = ConfigBuilder("spark.deploy.retainedDrivers")
    .version("1.1.0")
    .intConf
    .createWithDefault(200)

  val REAPER_ITERATIONS = ConfigBuilder("spark.dead.worker.persistence")
    .version("0.8.0")
    .intConf
    .createWithDefault(15)

  val MAX_EXECUTOR_RETRIES = ConfigBuilder("spark.deploy.maxExecutorRetries")
    .version("1.6.3")
    .intConf
    .createWithDefault(10)

  // （1）当spreadOutApps=true：使用轮流均摊的策略，也就是采用圆桌（round-robin）算法，
  // 图中的虚线表示第一次轮流摊派的资源不足以满足申请的需求，因此开始第二轮摊派，依次轮流均摊，直到符合资源需求。
  // （2）当spreadOutApps=false：使用依次全占策略，依次从可用Workers上获取该Worker上可用的全部资源，直到符合资源需求
  val SPREAD_OUT_APPS = ConfigBuilder("spark.deploy.spreadOut")
    .version("0.6.1")
    .booleanConf
    .createWithDefault(true)

  val DEFAULT_CORES = ConfigBuilder("spark.deploy.defaultCores")
    .version("0.9.0")
    .intConf
    .createWithDefault(Int.MaxValue)


}
