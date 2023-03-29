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

import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;

// 基于Tungsten Sort的Shuffle实现机制使用的ShuffleHandle与ShuffleWriter分别为SerializedShuffleHandle与UnsafeShuffleWriter
@Private
public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  @VisibleForTesting
  static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final BlockManager blockManager;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final ShuffleExecutorComponents shuffleExecutorComponents;
  private final int shuffleId;
  private final long mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;
  private final int initialSortBufferSize;
  private final int inputBufferSizeInBytes;

  @Nullable private MapStatus mapStatus;
  @Nullable private ShuffleExternalSorter sorter;
  private long peakMemoryUsedBytes = 0;

  /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  private MyByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  public UnsafeShuffleWriter(
      BlockManager blockManager,
      TaskMemoryManager memoryManager,
      SerializedShuffleHandle<K, V> handle,
      long mapId,
      TaskContext taskContext,
      SparkConf sparkConf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleExecutorComponents shuffleExecutorComponents) {
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      throw new IllegalArgumentException(
        "UnsafeShuffleWriter can only be used for shuffles with at most " +
        SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
        " reduce partitions");
    }
    this.blockManager = blockManager;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = writeMetrics;
    this.shuffleExecutorComponents = shuffleExecutorComponents;
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    this.initialSortBufferSize =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE());
    this.inputBufferSizeInBytes =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    open();
  }

  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * This convenience method should only be called in test code.
   */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(JavaConverters.asScalaIteratorConverter(records).asScala());
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    boolean success = false;
    try {
      // 对输入的记录集records，循环将每条记录插入到外部排序器
      while (records.hasNext()) {
        insertRecordIntoSorter(records.next());
      }
      // closeAndWriteOutput方法写数据文件与索引文件，在写的过程中，会先合并外部排序器在插入过程中生成的Spill中间文件
      closeAndWriteOutput();
      // 生成最终的两个结果文件，和Sorted Based Shuffle的实现机制一样，每个Map端的任务对应生成一个数据（Data）文件
      // 和对应的索引（Index）文件

      success = true;
    } finally {
      if (sorter != null) {
        try {
          // sorter.cleanupResources()最后释放外部排序器的资源
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error("In addition to a failure during writing, we failed during " +
                         "cleanup.", e);
          }
        }
      }
    }
  }

  private void open() {
    assert (sorter == null);
    sorter = new ShuffleExternalSorter(
      memoryManager,
      blockManager,
      taskContext,
      initialSortBufferSize,
      partitioner.numPartitions(),
      sparkConf,
      writeMetrics);
    serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert(sorter != null);
    updatePeakMemoryUsed();
    // 设为null，用于GC垃圾回收
    serBuffer = null;
    serOutputStream = null;
    // 关闭外部排序器，并获取全部Spill信息
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    sorter = null;
    // 通过块解析器获取输出文件名
    final long[] partitionLengths;
    try {
      // 合并spills
      partitionLengths = mergeSpills(spills);
    } finally {
      for (SpillInfo spill : spills) {
        if (spill.file.exists() && !spill.file.delete()) {
          logger.error("Error while deleting spill file {}", spill.file.getPath());
        }
      }
    }
    mapStatus = MapStatus$.MODULE$.apply(
      blockManager.shuffleServerId(), partitionLengths, mapId);
  }

  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert(sorter != null);
    // 对于多次访问的Key值，使用局部变量，可以避免多次函数调用
    final K key = record._1();
    final int partitionId = partitioner.getPartition(key);
    // 先复位存放每条记录的缓冲区，内部使用ByteArrayOutputStream存放的每条记录，容量为1MB
    serBuffer.reset();
    // 进一步使用序列化器从serBuffer缓冲区构建序列化输出流，将记录写入到缓冲区
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();

    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);
    // 将记录插入到外部排序器中，serBuffer是一个字节数组，内部数据存放的偏移量为Platform.BYTE_ARRAY_OFFSET

    sorter.insertRecord(
      serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  /**
   * Spark.Shuffle.compress参数是判断是否对mapper端的聚合输出进行压缩，
   * 默认是true，表示在每个Shuffle的过程中都会对mapper端的输出进行压缩。
   * 例如，说几千台或者上万台的机器进行汇聚计算，
   * 数据量和网络传输会非常大，这会造成大量内存消耗、
   * 磁盘I/O消耗和网络I/O消耗。此时如果在Mapper端进行了压缩，就会减少Shuffle过程中下一个Stage向上一个Stage抓数据的网络开销，
   * 大大地减轻Shuffle的压力
   * 。
   *
   *
   * Merge zero or more spill files together, choosing the fastest merging strategy based on the
   * number of spills and the IO compression codec.
   * 合并0个或多个Spill的中间文件，基于Spills的个数以及I/O压缩码选择最快速的合并策略，返回包含合并文件中各个分区的数据长度的数组
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpills(SpillInfo[] spills) throws IOException {
    long[] partitionLengths;
    if (spills.length == 0) {
      final ShuffleMapOutputWriter mapWriter = shuffleExecutorComponents
          .createMapOutputWriter(shuffleId, mapId, partitioner.numPartitions());
      return mapWriter.commitAllPartitions().getPartitionLengths();
    } else if (spills.length == 1) {
      Optional<SingleSpillShuffleMapOutputWriter> maybeSingleFileWriter =
          shuffleExecutorComponents.createSingleFileMapOutputWriter(shuffleId, mapId);
      if (maybeSingleFileWriter.isPresent()) {
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        partitionLengths = spills[0].partitionLengths;
        logger.debug("Merge shuffle spills for mapId {} with length {}", mapId,
            partitionLengths.length);
        maybeSingleFileWriter.get().transferMapSpillFile(spills[0].file, partitionLengths);
      } else {
        partitionLengths = mergeSpillsUsingStandardWriter(spills);
      }
    } else {
      partitionLengths = mergeSpillsUsingStandardWriter(spills);
    }
    return partitionLengths;
  }

  private long[] mergeSpillsUsingStandardWriter(SpillInfo[] spills) throws IOException {
    long[] partitionLengths;
    // 获取Shuffle的压缩配置信息
    final boolean compressionEnabled = (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_COMPRESS());
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    final boolean fastMergeEnabled =
        (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_UNSAFE_FAST_MERGE_ENABLE());
    // 获取是否启动unsafe的快速合并
    final boolean fastMergeIsSupported = !compressionEnabled ||
        CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    // 没有压缩或者当压缩码支持序列化流合并时，支持快速合并
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();
    final ShuffleMapOutputWriter mapWriter = shuffleExecutorComponents
        .createMapOutputWriter(shuffleId, mapId, partitioner.numPartitions());
    try {

      // There are multiple spills to merge, so none of these spill files' lengths were counted
      // towards our shuffle write count or shuffle write time. If we use the slow merge path,
      // then the final output file's size won't necessarily be equal to the sum of the spill
      // files' sizes. To guard against this case, we look at the output file's actual size when
      // computing shuffle bytes written.
      //
      // We allow the individual merge methods to report their own IO times since different merge
      // strategies use different IO techniques.  We count IO during merge towards the shuffle
      // write time, which appears to be consistent with the "not bypassing merge-sort" branch in
      // ExternalSorter.
      if (fastMergeEnabled && fastMergeIsSupported) {
        // Compression is disabled or we are using an IO compression codec that supports
        // decompression of concatenated compressed streams, so we can perform a fast spill merge
        // that doesn't need to interpret the spilled bytes.
        if (transferToEnabled && !encryptionEnabled) {
          logger.debug("Using transferTo-based fast merge");
          // 通过NIO的方式合并各个Spills的分区字节数据
          // 仅在 I/0压缩码和序列化器支持序列化流的合并时安全
          mergeSpillsWithTransferTo(spills, mapWriter);
        } else {

          logger.debug("Using fileStream-based fast merge");
          // 使用Java FileStreams文件流的方式合并
          mergeSpillsWithFileStream(spills, mapWriter, null);
        }
      } else {
        logger.debug("Using slow merge");
        mergeSpillsWithFileStream(spills, mapWriter, compressionCodec);
      }
      // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
      // in-memory records, we write out the in-memory records to a file but do not count that
      // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
      // to be counted as shuffle write, but this will lead to double-counting of the final
      // SpillInfo's bytes.
      //  更新Shuffle写数据的度量信息
      writeMetrics.decBytesWritten(spills[spills.length - 1].file.length());
      partitionLengths = mapWriter.commitAllPartitions().getPartitionLengths();
    } catch (Exception e) {
      try {
        mapWriter.abort(e);
      } catch (Exception e2) {
        logger.warn("Failed to abort writing the map output.", e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
    return partitionLengths;
  }

  /**
   * Merges spill files using Java FileStreams. This code path is typically slower than
   * the NIO-based merge, {@link UnsafeShuffleWriter#mergeSpillsWithTransferTo(SpillInfo[],
   * ShuffleMapOutputWriter)}, and it's mostly used in cases where the IO compression codec
   * does not support concatenation of compressed data, when encryption is enabled, or when
   * users have explicitly disabled use of {@code transferTo} in order to work around kernel bugs.
   * This code path might also be faster in cases where individual partition size in a spill
   * is small and UnsafeShuffleWriter#mergeSpillsWithTransferTo method performs many small
   * disk ios which is inefficient. In those case, Using large buffers for input and output
   * files helps reducing the number of disk ios, making the file merging faster.
   *
   * @param spills the spills to merge.
   * @param mapWriter the map output writer to use for output.
   * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
   * @return the partition lengths in the merged file.
   */
  private void mergeSpillsWithFileStream(
      SpillInfo[] spills,
      ShuffleMapOutputWriter mapWriter,
      @Nullable CompressionCodec compressionCodec) throws IOException {
    logger.debug("Merge shuffle spills with FileStream for mapId {}", mapId);
    final int numPartitions = partitioner.numPartitions();
    // 对应打开的输入流的个数为Spills的临时文件个数
    final InputStream[] spillInputStreams = new InputStream[spills.length];

    boolean threwException = true;
    try {
      // 为每个Spills中间文件打开文件输入流
      for (int i = 0; i < spills.length; i++) {
        spillInputStreams[i] = new NioBufferedFileInputStream(
          spills[i].file,
          inputBufferSizeInBytes);
        // Only convert the partitionLengths when debug level is enabled.
        if (logger.isDebugEnabled()) {
          logger.debug("Partition lengths for mapId {} in Spill {}: {}", mapId, i,
              Arrays.toString(spills[i].partitionLengths));
        }
      }
      // 遍历分区
      for (int partition = 0; partition < numPartitions; partition++) {
        boolean copyThrewException = true;
        ShufflePartitionWriter writer = mapWriter.getPartitionWriter(partition);
        // 屏蔽底层输出流的close（）调用，以便能够关闭高层流，以确保所有数据都真正刷新并清除内部状态
        OutputStream partitionOutput = writer.openStream();
        try {
          partitionOutput = new TimeTrackingOutputStream(writeMetrics, partitionOutput);
          partitionOutput = blockManager.serializerManager().wrapForEncryption(partitionOutput);
          if (compressionCodec != null) {
            partitionOutput = compressionCodec.compressedOutputStream(partitionOutput);
          }
          // 依次从各个Spills输入流中读取当前分区的数据长度指定个数的字节，到各个分区对应的输出文件流中
          for (int i = 0; i < spills.length; i++) {
            final long partitionLengthInSpill = spills[i].partitionLengths[partition];

            if (partitionLengthInSpill > 0) {
              InputStream partitionInputStream = null;
              boolean copySpillThrewException = true;
              try {
                partitionInputStream = new LimitedInputStream(spillInputStreams[i],
                    partitionLengthInSpill, false);
                partitionInputStream = blockManager.serializerManager().wrapForEncryption(
                    partitionInputStream);
                if (compressionCodec != null) {
                  partitionInputStream = compressionCodec.compressedInputStream(
                      partitionInputStream);
                }
                ByteStreams.copy(partitionInputStream, partitionOutput);
                copySpillThrewException = false;
              } finally {
                Closeables.close(partitionInputStream, copySpillThrewException);
              }
            }
          }
          copyThrewException = false;
        } finally {
          Closeables.close(partitionOutput, copyThrewException);
        }
        long numBytesWritten = writer.getNumBytesWritten();
        writeMetrics.incBytesWritten(numBytesWritten);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
    }
  }

  /**
   * Merges spill files by using NIO's transferTo to concatenate spill partitions' bytes.
   * This is only safe when the IO compression codec and serializer support concatenation of
   * serialized streams.
   *
   * @param spills the spills to merge.
   * @param mapWriter the map output writer to use for output.
   * @return the partition lengths in the merged file.
   */
  private void mergeSpillsWithTransferTo(
      SpillInfo[] spills,
      ShuffleMapOutputWriter mapWriter) throws IOException {
    logger.debug("Merge shuffle spills with TransferTo for mapId {}", mapId);
    final int numPartitions = partitioner.numPartitions();
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    final long[] spillInputChannelPositions = new long[spills.length];

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
        // Only convert the partitionLengths when debug level is enabled.
        if (logger.isDebugEnabled()) {
          logger.debug("Partition lengths for mapId {} in Spill {}: {}", mapId, i,
              Arrays.toString(spills[i].partitionLengths));
        }
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        boolean copyThrewException = true;
        ShufflePartitionWriter writer = mapWriter.getPartitionWriter(partition);
        WritableByteChannelWrapper resolvedChannel = writer.openChannelWrapper()
            .orElseGet(() -> new StreamFallbackChannelWrapper(openStreamUnchecked(writer)));
        try {
          for (int i = 0; i < spills.length; i++) {
            long partitionLengthInSpill = spills[i].partitionLengths[partition];
            final FileChannel spillInputChannel = spillInputChannels[i];
            final long writeStartTime = System.nanoTime();
            Utils.copyFileStreamNIO(
                spillInputChannel,
                resolvedChannel.channel(),
                spillInputChannelPositions[i],
                partitionLengthInSpill);
            copyThrewException = false;
            spillInputChannelPositions[i] += partitionLengthInSpill;
            writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
          }
        } finally {
          Closeables.close(resolvedChannel, copyThrewException);
        }
        long numBytes = writer.getNumBytesWritten();
        writeMetrics.incBytesWritten(numBytes);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (int i = 0; i < spills.length; i++) {
        assert(spillInputChannelPositions[i] == spills[i].file.length());
        Closeables.close(spillInputChannels[i], threwException);
      }
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        sorter.cleanupResources();
      }
    }
  }

  private static OutputStream openStreamUnchecked(ShufflePartitionWriter writer) {
    try {
      return writer.openStream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final class StreamFallbackChannelWrapper implements WritableByteChannelWrapper {
    private final WritableByteChannel channel;

    StreamFallbackChannelWrapper(OutputStream fallbackStream) {
      this.channel = Channels.newChannel(fallbackStream);
    }

    @Override
    public WritableByteChannel channel() {
      return channel;
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}
