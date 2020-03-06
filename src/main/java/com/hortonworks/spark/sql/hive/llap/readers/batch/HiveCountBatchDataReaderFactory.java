package com.hortonworks.spark.sql.hive.llap.readers.batch;

import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class HiveCountBatchDataReaderFactory implements DataReaderFactory<ColumnarBatch> {
  private long numRows;

  public HiveCountBatchDataReaderFactory(long numRows) {
    this.numRows = numRows;
  }

  @Override
  public DataReader<ColumnarBatch> createDataReader() {
    return new HiveCountBatchDataReader(numRows);
  }
}
