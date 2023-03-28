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

package org.apache.spark.unsafe.memory;

import javax.annotation.Nullable;

/**
 * 内存位置。进行跟踪
 * 通过内存地址跟踪(堆外内存分配)，或通过 JVM 对象的偏移量(堆内存分配)
 * A memory location. Tracked either by a memory address (with off-heap allocation),
 * or by an offset from a JVM object (on-heap allocation).
 */
public class MemoryLocation {

  // Off-Heap内存模式，obj为null，地址由64位的offset唯一标识
  // On-Heap内存模式，obj为堆中该对象的引用，offset为该对象中的偏移量
  @Nullable
  Object obj;

  long offset;

  // 一个内存地址，用于跟踪Off-Heap模式下的内存地址或On-Heap模式下的内存地址
  public MemoryLocation(@Nullable Object obj, long offset) {
    this.obj = obj;
    this.offset = offset;
  }

  public MemoryLocation() {
    this(null, 0);
  }

  public void setObjAndOffset(Object newObj, long newOffset) {
    this.obj = newObj;
    this.offset = newOffset;
  }

  public final Object getBaseObject() {
    return obj;
  }

  public final long getBaseOffset() {
    return offset;
  }
}
