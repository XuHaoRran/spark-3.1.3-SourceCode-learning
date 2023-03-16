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

package org.apache.spark.shuffle

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.BlockId

private[spark]
/**
 * Implementers of this trait understand how to retrieve block data for a logical shuffle block
 * identifier (i.e. map, reduce, and shuffle). Implementations may use files or file segments to
 * encapsulate shuffle data. This is used by the BlockStore to abstract over different shuffle
 * implementations when shuffle data is retrieved.
 *
 * 该特质的具体实现子类知道如何通过一个逻辑Shuffle块标识信息来获取一个块数据。
 * 具体实现可以使用文件或文件段来封装Shuffle的数据。
 * 这是获取Shuffle块数据时使用的抽象接口，在BlockStore中使用
 *
 * 目前在ShuffleBlockResolver的各个具体子类中，除给出获取数据的接口外，
 * 通常会提供如何解析块数据信息的接口，即提供了写数据块时的物理块与逻辑块之间映射关系的解析方法。
 *
 */
trait ShuffleBlockResolver {
  type ShuffleId = Int

  /**
   * Retrieve the data for the specified block.
   *
   * When the dirs parameter is None then use the disk manager's local directories. Otherwise,
   * read from the specified directories.
   *
   * If the data for that block is not available, throws an unspecified exception.
   *
   * 获取指定块的数据，如果指定块的数据无法获取，则抛出异常
   */
  def getBlockData(blockId: BlockId, dirs: Option[Array[String]] = None): ManagedBuffer

  def stop(): Unit
}
