package com.hortonworks.spark.sql.hive.llap;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.*;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Driver:
 *   UserCode -> HiveWarehouseConnector -> HiveWarehouseDataSourceReader -> HiveWarehouseInputPartition
 * Task serializer:
 *   HiveWarehouseInputPartition (Driver) -> bytes -> HiveWarehouseInputPartition (Executor task)
 * Executor:
 *   HiveWarehouseInputPartition -> HiveWarehouseInputPartitionReader
 */
public class HiveWarehouseConnector implements DataSourceV2, ReadSupport, SessionConfigSupport, WriteSupport {

  private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseConnector.class);

  //one reader per instance of HiveWarehouseConnector
  //this will be returned everytime createReader() is invoked on same HiveWarehouseConnector instance
  //this is to avoid the problems occurred due to multiple reader initialization.
  //http://apache-spark-user-list.1001560.n3.nabble.com/DataSourceV2-APIs-creating-multiple-instances-of-DataSourceReader-and-hence-not-preserving-the-state-tc33646.html
  private HiveWarehouseDataSourceReader reader = null;

  @Override public DataSourceReader createReader(DataSourceOptions options) {
    try {
      return getDataSourceReader(getOptions(options));
    } catch (IOException e) {
      LOG.error("Error creating {}", getClass().getName());
      LOG.error(ExceptionUtils.getStackTrace(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String jobId, StructType schema,
      SaveMode mode, DataSourceOptions options) {
    Map<String, String> params = getOptions(options);
    String stagingDirPrefix = HWConf.LOAD_STAGING_DIR.getFromOptionsMap(params);
    Path path = new Path(stagingDirPrefix);
    Configuration conf = SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
    return Optional.of(getDataSourceWriter(jobId, schema, path, params, conf, mode));
  }

  @Override public String keyPrefix() {
    return HiveWarehouseSession.HIVE_WAREHOUSE_POSTFIX;
  }

  private static Map<String, String> getOptions(DataSourceOptions options) {
    return options.asMap();
  }

  protected DataSourceReader getDataSourceReader(Map<String, String> params) throws IOException {
    if (reader == null) {
      reader = new HiveWarehouseDataSourceReader(params);
    }
    return reader;
  }

  protected DataSourceWriter getDataSourceWriter(String jobId, StructType schema,
                                                 Path path, Map<String, String> options, Configuration conf, SaveMode mode) {
    return new HiveWarehouseDataSourceWriter(options, jobId, schema, path, conf, mode);
  }

}
