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

package org.apache.spark.shuffle.sort;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.Optional;
import javax.annotation.Nullable;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.internal.config.package$;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;

/**
 * This class implements sort-based shuffle's hash-style shuffle fallback path. This write path
 * writes incoming records to separate files, one file per reduce partition, then concatenates these
 * per-partition files to form a single output file, regions of which are served to reducers.
 * Records are not buffered in memory. It writes output in a format
 * that can be served / consumed via {@link org.apache.spark.shuffle.IndexShuffleBlockResolver}.
 * <p>
 * This write path is inefficient for shuffles with large numbers of reduce partitions because it
 * simultaneously opens separate serializers and file streams for all partitions. As a result,
 * {@link SortShuffleManager} only selects this write path when
 * <ul>
 *    <li>no map-side combine is specified, and</li>
 *    <li>the number of partitions is less than or equal to
 *      <code>spark.shuffle.sort.bypassMergeThreshold</code>.</li>
 * </ul>
 *
 * This code used to be part of {@link org.apache.spark.util.collection.ExternalSorter} but was
 * refactored into its own class in order to reduce code complexity; see SPARK-7855 for details.
 * <p>
 * There have been proposals to completely remove this code path; see SPARK-6026 for details.
 *
 * 该类实现了带Hash风格的基于Sort的Shuffle机制，为每个Reduce端的任务构建一个输出文件，
 * 将输入的每条记录分别写入各自对应的文件中，并在最后将这些基于各个分区的文件合并成一个输出文件
 *
 * 在Reducer端任务数比较少的情况下，基于Hash的Shuffle实现机制明显比基于Sort的Shuffle实现机制要快，
 * 因此基于Sort的Shuffle实现机制提供了一个fallback方案，
 * 对于Reducer端任务数少于配置属性spark.shuffle.sort.bypassMergeThreshold设置的个数时，
 * 使用带Hash风格的fallback计划，由BypassMergeSortShuffleWriter具体实现。
 *
 * 使用该写入器的条件如下。
 * （1）不能指定Ordering，从前面数据读取器的解析可以知道，当指定Ordering时，会对分区内部的数据进行排序。
 * 因此，对应的BypassMergeSortShuffleWriter写入器避免了排序开销。
 * （2）不能指定Aggregator。
 * （3）分区个数小于spark.shuffle.sort.bypassMergeThreshold配置属性指定的个数。
 */
final class BypassMergeSortShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(BypassMergeSortShuffleWriter.class);

  private final int fileBufferSize;
  private final boolean transferToEnabled;
  private final int numPartitions;
  private final BlockManager blockManager;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final int shuffleId;
  private final long mapId;
  private final Serializer serializer;
  private final ShuffleExecutorComponents shuffleExecutorComponents;

  /** Array of file writers, one for each partition */
  private DiskBlockObjectWriter[] partitionWriters;
  private FileSegment[] partitionWriterSegments;
  @Nullable private MapStatus mapStatus;
  private long[] partitionLengths;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  BypassMergeSortShuffleWriter(
      BlockManager blockManager,
      BypassMergeSortShuffleHandle<K, V> handle,
      long mapId,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleExecutorComponents shuffleExecutorComponents) {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSize = (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.transferToEnabled = conf.getBoolean("spark.file.transferTo", true);
    this.blockManager = blockManager;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.mapId = mapId;
    this.shuffleId = dep.shuffleId();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = writeMetrics;
    this.serializer = dep.serializer();
    this.shuffleExecutorComponents = shuffleExecutorComponents;
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    // 为每个Reduce端的分区打开的DiskBlockObjectWriter存放于partitionWriters，需要根据具体的Reduce端的分区个数进行构建
    assert (partitionWriters == null);
    ShuffleMapOutputWriter mapOutputWriter = shuffleExecutorComponents
        .createMapOutputWriter(shuffleId, mapId, numPartitions);
    try {
      if (!records.hasNext()) {
        partitionLengths = mapOutputWriter.commitAllPartitions().getPartitionLengths();
        // 下面代码的调用形式是对应在Java类中调用Scala提供的object中的applay方法的形式，是由编译器编译Scala中的object得到的结果来决定的
        mapStatus = MapStatus$.MODULE$.apply(
          blockManager.shuffleServerId(), partitionLengths, mapId);
        return;
      }
      final SerializerInstance serInstance = serializer.newInstance();
      final long openStartTime = System.nanoTime();
      // 对应每个分区各配置一个磁盘写入器DisBLockObjectWriter
      partitionWriters = new DiskBlockObjectWriter[numPartitions];
      partitionWriterSegments = new FileSegment[numPartitions];
      // 注意，在该写入方式下，会同时打开numPartitions个DiskBlockObjectWriter，
      // 因此对应的分区数不应设置过大，避免带来过大的内存开销目前对应的DisBlockObjectWriter的缓存默认大小
      // 配置为32KB，比早先的100KB降低了很多，但也说明不适合同时打开太多的DiskBlockObjectWriter实例
      for (int i = 0; i < numPartitions; i++) {
        final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
                // 其中调用的createTempShuffleBlock方法描述了各个分区生成的中间临时文件的格式与对应的BlockId
            blockManager.diskBlockManager().createTempShuffleBlock();
        final File file = tempShuffleBlockIdPlusFile._2();
        final BlockId blockId = tempShuffleBlockIdPlusFile._1();
        partitionWriters[i] =
            blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, and can take a long time in aggregate when we open many files, so should be
      // included in the shuffle write time.
      // 创建文件写入和创建磁盘写入器都涉及与磁盘的交互，当打开许多文件时，磁盘写会花费很长时间，所以磁盘写入时间应包含在Shuffle写入时间内
      writeMetrics.incWriteTime(System.nanoTime() - openStartTime);
      // 读取每条记录，并根据分区器将该记录交由分区对应的DiskBlockObjectWriter写入各自对应的临时文件
      while (records.hasNext()) {
        final Product2<K, V> record = records.next();
        final K key = record._1();
        // 根据分区器写文件
        partitionWriters[partitioner.getPartition(key)].write(key, record._2());
      }

      for (int i = 0; i < numPartitions; i++) {
        try (DiskBlockObjectWriter writer = partitionWriters[i]) {
          partitionWriterSegments[i] = writer.commitAndGet();
        }
      }
      // 将所有按分区的文件连接到一个单独的组合文件中，返回：文件的每个分区的长度数组（以字节为单位）（由map output tracker使用）
      partitionLengths = writePartitionedData(mapOutputWriter);
      // 封装并返回任务结果
      mapStatus = MapStatus$.MODULE$.apply(
        blockManager.shuffleServerId(), partitionLengths, mapId);
    } catch (Exception e) {
      try {
        mapOutputWriter.abort(e);
      } catch (Exception e2) {
        logger.error("Failed to abort the writer after failing to write map output.", e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
  }

  @VisibleForTesting
  long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private long[] writePartitionedData(ShuffleMapOutputWriter mapOutputWriter) throws IOException {
    // Track location of the partition starts in the output file
    if (partitionWriters != null) {
      final long writeStartTime = System.nanoTime();
      try {
        for (int i = 0; i < numPartitions; i++) {
          final File file = partitionWriterSegments[i].file();
          ShufflePartitionWriter writer = mapOutputWriter.getPartitionWriter(i);
          if (file.exists()) {
            if (transferToEnabled) {
              // Using WritableByteChannelWrapper to make resource closing consistent between
              // this implementation and UnsafeShuffleWriter.
              Optional<WritableByteChannelWrapper> maybeOutputChannel = writer.openChannelWrapper();
              if (maybeOutputChannel.isPresent()) {
                writePartitionedDataWithChannel(file, maybeOutputChannel.get());
              } else {
                writePartitionedDataWithStream(file, writer);
              }
            } else {
              writePartitionedDataWithStream(file, writer);
            }
            if (!file.delete()) {
              logger.error("Unable to delete file for partition {}", i);
            }
          }
        }
      } finally {
        writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
      }
      partitionWriters = null;
    }
    return mapOutputWriter.commitAllPartitions().getPartitionLengths();
  }

  private void writePartitionedDataWithChannel(
      File file,
      WritableByteChannelWrapper outputChannel) throws IOException {
    boolean copyThrewException = true;
    try {
      FileInputStream in = new FileInputStream(file);
      try (FileChannel inputChannel = in.getChannel()) {
        Utils.copyFileStreamNIO(
            inputChannel, outputChannel.channel(), 0L, inputChannel.size());
        copyThrewException = false;
      } finally {
        Closeables.close(in, copyThrewException);
      }
    } finally {
      Closeables.close(outputChannel, copyThrewException);
    }
  }

  private void writePartitionedDataWithStream(File file, ShufflePartitionWriter writer)
      throws IOException {
    boolean copyThrewException = true;
    FileInputStream in = new FileInputStream(file);
    OutputStream outputStream;
    try {
      outputStream = writer.openStream();
      try {
        Utils.copyStream(in, outputStream, false, false);
        copyThrewException = false;
      } finally {
        Closeables.close(outputStream, copyThrewException);
      }
    } finally {
      Closeables.close(in, copyThrewException);
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // The map task failed, so delete our output data.
        if (partitionWriters != null) {
          try {
            for (DiskBlockObjectWriter writer : partitionWriters) {
              // This method explicitly does _not_ throw exceptions:
              File file = writer.revertPartialWritesAndClose();
              if (!file.delete()) {
                logger.error("Error while deleting file {}", file.getAbsolutePath());
              }
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}
