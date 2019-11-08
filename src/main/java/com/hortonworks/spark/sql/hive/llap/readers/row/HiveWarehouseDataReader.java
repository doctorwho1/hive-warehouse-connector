package com.hortonworks.spark.sql.hive.llap.readers.row;

import com.hortonworks.spark.sql.hive.llap.RowConverter;
import com.hortonworks.spark.sql.hive.llap.common.CommonBroadcastInfo;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import org.apache.hadoop.hive.llap.LlapArrowBatchRecordReader;
import org.apache.hadoop.hive.llap.LlapArrowRowRecordReader;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import java.io.IOException;

public class HiveWarehouseDataReader implements DataReader<Row> {

  private LlapArrowRowRecordReader reader;
  private org.apache.hadoop.hive.llap.Row currentLlapRow;
  private String attemptId;
  private boolean inputExhausted = false;

  public HiveWarehouseDataReader(LlapInputSplit split, JobConf conf,
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
    this.currentLlapRow = this.reader.createValue();
  }

  private LlapArrowRowRecordReader getRecordReader(LlapInputSplit split, JobConf conf, long arrowAllocatorMax)
          throws IOException {
    LlapArrowBatchRecordReader batchReader
            = (LlapArrowBatchRecordReader)JobUtil.getLlapArrowBatchRecordReader(
                    split, conf, arrowAllocatorMax, attemptId);
    return new LlapArrowRowRecordReader(conf, batchReader.getSchema(), batchReader);
  }

  @Override
  public boolean next() throws IOException {
    // don't call reader.next() if the input is already exhausted or else it will block.
    // see https://hortonworks.jira.com/browse/BUG-120380 for more on it.
    if (inputExhausted) {
      return false;
    }
    boolean hasNextRow = reader.next(null, currentLlapRow);
    inputExhausted = !hasNextRow;
    return hasNextRow;
  }

  @Override
  public Row get() {
    return RowConverter.llapRowToSparkRow(currentLlapRow, reader.getSchema());
  }

  @Override
  public void close() throws IOException {
    //reader.close() will throw exception unless all arrow buffers have been released
    //See org.apache.hadoop.hive.llap.close()
    //See org.apache.arrow.memory.BaseAllocator.close()
    if(this.reader != null) {
      this.reader.close();
    }
  }

}

