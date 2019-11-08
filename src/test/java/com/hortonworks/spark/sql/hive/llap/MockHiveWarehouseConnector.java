package com.hortonworks.spark.sql.hive.llap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.hortonworks.spark.sql.hive.llap.common.CommonBroadcastInfo;
import com.hortonworks.spark.sql.hive.llap.common.HWConf;
import com.hortonworks.spark.sql.hive.llap.readers.HiveDataSourceReaderForSpark23X;
import com.hortonworks.spark.sql.hive.llap.readers.PrunedFilteredHiveDataSourceReader;
import com.hortonworks.spark.sql.hive.llap.readers.batch.HiveWarehouseBatchDataReader;
import com.hortonworks.spark.sql.hive.llap.readers.batch.HiveWarehouseBatchDataReaderFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.hive.ql.io.arrow.RootAllocatorFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class MockHiveWarehouseConnector extends HiveWarehouseConnector {

  public static int[] testVector = {1, 2, 3, 4, 5};
  public static Map<String, Object> writeOutputBuffer = new HashMap<>();
  public static long COUNT_STAR_TEST_VALUE = 1024;

  @Override
  protected DataSourceReader getDataSourceReader(Map<String, String> params) throws IOException {
    return new MockHiveWarehouseDataSourceReader(params);
  }

  @Override
  protected DataSourceWriter getDataSourceWriter(String jobId, StructType schema,
      Path path, Map<String, String> options, Configuration conf, SaveMode mode) {
    return new MockWriteSupport.MockHiveWarehouseDataSourceWriter(options, jobId, schema, path, conf, mode);
  }

  public static class MockHiveWarehouseBatchDataReader extends HiveWarehouseBatchDataReader {

    public MockHiveWarehouseBatchDataReader(LlapInputSplit split, JobConf conf,
                                            long arrowAllocatorMax) throws Exception {
      super(split, conf, arrowAllocatorMax);
    }

    @Override
    protected RecordReader<?, ArrowWrapperWritable> getRecordReader(LlapInputSplit split, JobConf conf,
                                                                    long arrowAllocatorMax) throws IOException {
       return new MockLlapArrowBatchRecordReader(arrowAllocatorMax);
    }
  }

  public static class MockHiveWarehouseBatchDataReaderFactory extends HiveWarehouseBatchDataReaderFactory {


    public MockHiveWarehouseBatchDataReaderFactory(InputSplit split, JobConf jobConf, long arrowAllocatorMax) {
    }

    @Override
    public DataReader<ColumnarBatch> createDataReader() {
      try {
        return getDataReader(null, new JobConf(), Long.MAX_VALUE, null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected DataReader<ColumnarBatch> getDataReader(LlapInputSplit split, JobConf jobConf, long arrowAllocatorMax,
                                                      CommonBroadcastInfo commonBroadcastInfo)
        throws Exception {
      return new MockHiveWarehouseBatchDataReader(split, jobConf, arrowAllocatorMax);
    }
  }

  public static class MockHiveDataSourceReader extends PrunedFilteredHiveDataSourceReader {

    public MockHiveDataSourceReader(Map<String, String> options) throws IOException {
      super(options);
    }

    @Override
    protected StructType getTableSchema() throws Exception {
      return StructType.fromDDL("a INTEGER");
    }

    @Override
    protected List<DataReaderFactory<ColumnarBatch>> getDataReaderSplitsFactories(String query) {
      return Lists.newArrayList(new MockHiveWarehouseBatchDataReaderFactory(null, (JobConf) null, 0));
    }

    @Override
    protected long getCount(String query) {
      //Mock out the call to HS2 to get the count
      return COUNT_STAR_TEST_VALUE;
    }
  }

  public static class MockLlapArrowBatchRecordReader implements RecordReader<ArrowWrapperWritable, ArrowWrapperWritable> {

    VectorSchemaRoot vectorSchemaRoot;
    boolean hasNext = true;

    public MockLlapArrowBatchRecordReader(long arrowAllocatorMax) {
      BufferAllocator allocator = RootAllocatorFactory.INSTANCE.getOrCreateRootAllocator(arrowAllocatorMax);
      IntVector vector = new IntVector("a", allocator);
      vector.allocateNewSafe();
      for(int i = 0; i < testVector.length; i++) {
        vector.set(i, testVector[i]);
      }
      vector.setValueCount(testVector.length);
      List<Field> fields = Lists.newArrayList(vector.getField());
      List<FieldVector> vectors = new ArrayList<>();
      vectors.add(vector);
      vectorSchemaRoot = new VectorSchemaRoot(fields, vectors, testVector.length);
    }

    @Override public boolean next(ArrowWrapperWritable empty, ArrowWrapperWritable value)
        throws IOException {
      if(hasNext) {
        value.setVectorSchemaRoot(vectorSchemaRoot);
        hasNext = false;
        return true;
      }
      return hasNext;
    }

    @Override public ArrowWrapperWritable createKey() {
      return null;
    }
    @Override public ArrowWrapperWritable createValue() {
      return null;
    }
    @Override public long getPos() throws IOException { return 0; }
    @Override public void close() throws IOException { }
    @Override public float getProgress() throws IOException { return 0; }
  }
}
