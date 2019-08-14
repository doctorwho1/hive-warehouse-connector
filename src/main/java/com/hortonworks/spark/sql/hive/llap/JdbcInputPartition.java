package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.util.Map;

public class JdbcInputPartition implements InputPartition<InternalRow> {
  private final String query;
  private final Map<String, String> options;

  // No-arg constructor for executors
  public JdbcInputPartition() {
    this.query = null;
    this.options = null;
  }

  // Driver-side setup
  JdbcInputPartition(String query, Map<String, String> options) {
    this.query = query;
    this.options = options;
  }

  @Override
  public InputPartitionReader<InternalRow> createPartitionReader() {
    try {
      return new JdbcInputPartitionReader(query, options);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
