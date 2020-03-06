package com.hortonworks.spark.sql.hive.llap.readers.batch;

import com.hortonworks.spark.sql.hive.llap.common.CommonBroadcastInfo;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import org.apache.arrow.vector.FieldVector;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HiveWarehouseBatchDataReader implements DataReader<ColumnarBatch> {

  private RecordReader<?, ArrowWrapperWritable> reader;
  private ArrowWrapperWritable wrapperWritable = new ArrowWrapperWritable();
  //Reuse single instance of ColumnarBatch and ColumnVector[]
  //See org.apache.spark.sql.vectorized.ColumnarBatch
  //"Instance of it is meant to be reused during the entire data loading process."
  private ColumnarBatch columnarBatch;
  private ColumnVector[] columnVectors;
  private String attemptId;
  private boolean inputExhausted = false;

  public HiveWarehouseBatchDataReader(LlapInputSplit split,
                                      JobConf conf,
                                      long arrowAllocatorMax,
                                      CommonBroadcastInfo commonBroadcastInfo) throws Exception {
    commonBroadcastInfo.getPlanSplit().assertValid();
    split.setPlanBytes(commonBroadcastInfo.getPlanSplit().getValue().getLlapInputSplit().getPlanBytes());
    commonBroadcastInfo.getSchemaSplit().assertValid();
    split.setSchema(commonBroadcastInfo.getSchemaSplit().getValue().getLlapInputSplit().getSchema());

    // Set TASK_ATTEMPT_ID to submit to LlapOutputFormatService
    this.attemptId = JobUtil.getTaskAttemptID(split).toString();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, this.attemptId);
    this.reader = getRecordReader(split, conf, arrowAllocatorMax);
  }

  // Used only for test.
  protected HiveWarehouseBatchDataReader(LlapInputSplit split, JobConf conf, long arrowAllocatorMax)
          throws Exception {
    this.reader = getRecordReader(split, conf, arrowAllocatorMax);
  }

  protected RecordReader<?, ArrowWrapperWritable> getRecordReader(
          LlapInputSplit split, JobConf conf, long arrowAllocatorMax) throws IOException {
    return JobUtil.getLlapArrowBatchRecordReader(split, conf, arrowAllocatorMax, attemptId);
  }

  @Override
  public boolean next() throws IOException {
    // don't call reader.next() if the input is already exhausted or else it will block.
    // see https://hortonworks.jira.com/browse/BUG-120380 for more on it.
    if (inputExhausted) {
      return false;
    }
    boolean hasNextBatch = reader.next(null, wrapperWritable);
    inputExhausted = !hasNextBatch;
    return hasNextBatch;
  }

  @Override
  public ColumnarBatch get() {
    //Spark asks you to convert one column at a time so that different
    //column types can be handled differently.
    //NumOfCols << NumOfRows so this is negligible
    List<FieldVector> fieldVectors = wrapperWritable.getVectorSchemaRoot().getFieldVectors();
    if(columnVectors == null) {
      //Lazy create ColumnarBatch/ColumnVector[] instance
      columnVectors = new ColumnVector[fieldVectors.size()];
      columnarBatch = new ColumnarBatch(columnVectors);
    }
    Iterator<FieldVector> iterator = fieldVectors.iterator();
    int rowCount = -1;
    for (int i = 0; i < columnVectors.length; i++) {
      FieldVector fieldVector = iterator.next();
      columnVectors[i] = new ArrowColumnVector(fieldVector);
      if (rowCount == -1) {
        //All column vectors have same length so we can get rowCount from any column
        rowCount = fieldVector.getValueCount();
      }
    }
    columnarBatch.setNumRows(rowCount);
    return columnarBatch;
  }

  @Override
  public void close() throws IOException {
    //close() single ColumnarBatch instance
    if(this.columnarBatch != null) {
      this.columnarBatch.close();
    }
    //reader.close() will throw exception unless all arrow buffers have been released
    //See org.apache.hadoop.hive.llap.close()
    //See org.apache.arrow.memory.BaseAllocator.close()
    if(this.reader != null) {
      this.reader.close();
    }
  }

}

