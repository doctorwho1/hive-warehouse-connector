package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class JdbcInputPartition implements InputPartition<InternalRow> {
  private final String query;
  private final Map<String, String> options;
  private final StructType schema;

  // No-arg constructor for executors
  public JdbcInputPartition() {
    this.query = null;
    this.options = null;
    this.schema = null;
  }

  // Driver-side setup
  JdbcInputPartition(String query, Map<String, String> options, StructType schema) {
    this.query = query;
    this.options = options;
    this.schema = schema;
  }

  @Override
  public InputPartitionReader<InternalRow> createPartitionReader() {
    try {
      return new JdbcInputPartitionReader(query, options, schema);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
