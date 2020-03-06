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

package com.hortonworks.spark.sql.hive.llap.writers;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper;
import com.hortonworks.spark.sql.hive.llap.common.Column;
import com.hortonworks.spark.sql.hive.llap.common.DescribeTableOutput;
import com.hortonworks.spark.sql.hive.llap.query.builder.DataWriteQueryBuilder;
import com.hortonworks.spark.sql.hive.llap.common.HWConf;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import com.hortonworks.spark.sql.hive.llap.util.SerializableHadoopConfiguration;
import com.hortonworks.spark.sql.hive.llap.util.SparkToHiveRecordMapper;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import scala.Option;

/**
 * Data source writer implementation to facilitate creation of {@link DataWriterFactory} and drive the writing process.
 * <br/>
 * It also decides how the record is to be written based on following parameters/factors:
 * <br/><br/>
 * 1) <b>SaveMode:</b> Implementation of SaveMode is same as to the one defined by spark.
 * <br/>Refer: <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes">https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes</a> .
 * <br/>
 * For {@link SaveMode#Append}, when the table does not already exist, it creates one and then writes.
 * <br/>
 * <br/>
 * 2) <b>Dataframe Schema and Hive Table Schema:</b>
 * <br/>
 * Writer will try to rearrange fields of dataframe in the same order as hive table columns if following conditions hold:
 * <br/>
 * 2.1) Specified table exists in hive
 * <br/>
 * 2.2) All the column names in dataframe are identical to those of hive table.
 * <br/>
 * 2.3) Order of columns in dataframe is different to that of in hive table.
 */
public class HiveWarehouseDataSourceWriter implements SupportsWriteInternalRow {

  private static final String SER_DE_LIBRARY = "SerDe Library:";
  private static final String ORC_SERDE = "OrcSerde";

  protected String jobId;
  protected StructType schema;
  private SaveMode mode;
  protected Path path;
  protected Configuration conf;
  protected Map<String, String> options;
  private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataSourceWriter.class);

  private boolean tableExists;
  private String tableName;
  private String databaseName;
  private SparkToHiveRecordMapper sparkToHiveRecordMapper;
  private boolean strictColumnNamesMapping;
  private DescribeTableOutput describeTableOutput;

  public HiveWarehouseDataSourceWriter(Map<String, String> options, String jobId, StructType schema,
                                       Path path, Configuration conf, SaveMode mode) {
    this.options = options;
    this.jobId = jobId;
    this.schema = schema;
    this.mode = mode;
    this.path = new Path(path, jobId);
    try {
      if (this.path.getFileSystem(conf).exists(this.path)) {
        logInfo("DataSourceWriter " + this + " found the target directory (" + this.path + "} already exists");
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to access the file system for the path (" + this.path + ") due to " + e.getMessage(), e);
    }
    this.conf = conf;
    populateDBTableNames(options.get("database"));
    this.strictColumnNamesMapping =
        BooleanUtils.toBoolean(com.hortonworks.spark.sql.hive.llap.common.HWConf.WRITE_PATH_STRICT_COLUMN_NAMES_MAPPING.getFromOptionsMap(options));
  }

  private void populateDBTableNames(String optionDatabase) {
    String database = optionDatabase;
    if(database == null) {
      database = com.hortonworks.spark.sql.hive.llap.common.HWConf.DEFAULT_DB.getFromOptionsMap(options);
    }
    String table = options.get("table");
    SchemaUtil.TableRef tableRef = SchemaUtil.getDbTableNames(database, table);
    this.tableName = tableRef.tableName;
    this.databaseName = tableRef.databaseName;
  }

  @Override
  public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
    String hiveCols[] = null;
    try (Connection connection = getConnection()) {
      this.tableExists = DefaultJDBCWrapper.tableExists(connection, databaseName, tableName);
      if (this.tableExists) {
        this.describeTableOutput = DefaultJDBCWrapper.describeTable(connection, databaseName, tableName);
        validateTableIsOrcFormatted();
        hiveCols = new String[describeTableOutput.getColumns().size() + describeTableOutput.getPartitionedColumns().size()];
        int i = 0;
        for (Column column : describeTableOutput.getColumns()) {
          hiveCols[i++] = column.getName();
        }
        for (Column column : describeTableOutput.getPartitionedColumns()) {
          hiveCols[i++] = column.getName();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.sparkToHiveRecordMapper = new SparkToHiveRecordMapper(schema, hiveCols);
    return new HiveWarehouseDataWriterFactory(jobId, schema, path, new SerializableHadoopConfiguration(conf), sparkToHiveRecordMapper);
  }

  private void validateTableIsOrcFormatted() {
    String serdeLib = describeTableOutput.findByColNameInStorageInfo(SER_DE_LIBRARY, true).getDataType();
    Preconditions.checkArgument(serdeLib.endsWith(ORC_SERDE),
        "Writes for non-ORC tables are not supported. Table: " + tableName + " is not an ORC table");
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    boolean needLoadData = (messages.length > 0);
    if (needLoadData) {
      // The target directory can have stale data files due to abnormal failures in previous execution.
      // Such files should be removed before loading them into Hive table.
      verifyAndCleanTargetDirectory(messages);
    }
    try {
      String url = com.hortonworks.spark.sql.hive.llap.common.HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options);
      String user = com.hortonworks.spark.sql.hive.llap.common.HWConf.USER.getFromOptionsMap(options);
      String dbcp2Configs = com.hortonworks.spark.sql.hive.llap.common.HWConf.DBCP2_CONF.getFromOptionsMap(options);
      String database = com.hortonworks.spark.sql.hive.llap.common.HWConf.DEFAULT_DB.getFromOptionsMap(options);
      String table = options.get("table");
      SchemaUtil.TableRef tableRef = SchemaUtil.getDbTableNames(database, table);
      database = tableRef.databaseName;
      table = tableRef.tableName;
      try (Connection conn = DefaultJDBCWrapper.getConnector(Option.empty(), url, user, dbcp2Configs)) {
        handleWriteWithSaveMode(database, table, conn, needLoadData);
      } catch (java.sql.SQLException e) {
        throw new RuntimeException(e);
      }
    } finally {
      try {
        path.getFileSystem(conf).delete(path, true);
      } catch(Exception e) {
        LOG.warn("Failed to cleanup temp dir {}", path.toString());
      }
      LOG.info("Commit job {}", jobId);
    }
  }

  private void verifyAndCleanTargetDirectory(WriterCommitMessage[] messages) {
    // Assumptions:
    // 1. The input messages have only one commit message per writer.
    // 2. Any written file is not duplicated in multiple commit messages.
    // 3. All the written files are directly present inside the target directory this.path.
    Map<String, Path> writtenFilesMap = new HashMap<>(messages.length);
    for (WriterCommitMessage wcm : messages) {
      Path writtenFilePath = ((SimpleWriterCommitMessage)wcm).getWrittenFilePath();
      String writtenFileName = writtenFilePath.getName();
      writtenFilesMap.put(writtenFileName, writtenFilePath);
      logInfo("Committed File " + writtenFilePath);
    }

    try {
      // All the files are present in target directory and hence need not traverse recursively.
      // If any directory exist, then just delete it.
      FileSystem fs = this.path.getFileSystem(conf);
      RemoteIterator<LocatedFileStatus> actualFiles = fs.listFiles(this.path, false);
      while (actualFiles.hasNext()) {
        LocatedFileStatus fileStatus = actualFiles.next();
        Path filePath = fileStatus.getPath();
        if (fileStatus.isDirectory() || (writtenFilesMap.get(filePath.getName()) == null)) {
          logInfo("Deleting invalid file/directory " + filePath);
          fs.delete(filePath, true);
        } else {
          if (fileStatus.getLen() == 0) {
            throw new IllegalStateException("Writer wrote 0 length data file (" + filePath + ") which is not valid.");
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to traverse the target directory " + this.path + " due to " + e.getMessage(), e);
    }
  }

  private void handleWriteWithSaveMode(String database, String table, Connection conn, boolean needLoadData) {
    boolean createTable = false;
    boolean loadData = needLoadData;
    boolean overwrite = false;
    switch (mode) {
      case ErrorIfExists:
        if (tableExists) {
          throw new IllegalArgumentException("Table[" + table + "] already exists, please specify a different SaveMode");
        }
        createTable = true;
        break;
      //same behavior for overwrite and append apart from `overwrite` flag
      case Overwrite:
        overwrite = true;
      case Append:
        //create if table does not exist
        //https://lists.apache.org/thread.html/40e6630f608802b31e6fdc537cf3ddbc07fe2d48c2ad51df91ae6e67@%3Cdev.spark.apache.org%3E
        if (!tableExists) {
          createTable = true;
        }
        break;
      case Ignore:
        //NO-OP if table already exists
        if (tableExists) {
          createTable = false;
          loadData = false;
        } else {
          createTable = true;
        }
        break;
    }

    LOG.info("Handling write: database:{}, table:{}, savemode: {}, tableExists:{}, createTable:{}, loadData:{}",
        database, table, mode, tableExists, createTable, loadData);

    if (loadData || createTable) {
      // check for column names and order equivalence in case the table exists.
      boolean validateAgainstHiveCols = tableExists && strictColumnNamesMapping;

      DataWriteQueryBuilder builder = new DataWriteQueryBuilder(database, table, path.toString(), jobId,
          sparkToHiveRecordMapper.getSchemaInHiveColumnsOrder())
          .withOverwriteData(overwrite)
          .withPartitionSpec(options.get(com.hortonworks.spark.sql.hive.llap.common.HWConf.PARTITION_OPTION_KEY))
          .withCreateTableQuery(createTable)
          .withStorageFormat("ORC")
          .withValidateAgainstHiveColumns(validateAgainstHiveCols)
          .withDescribeTableOutput(describeTableOutput)
          .withLoadData(loadData)
          ;
      List<String> loadDataQueries = builder.build();

      if (builder.isDynamicPartitionPresent()) {
        DefaultJDBCWrapper.setSessionLevelProps(conn, "hive.exec.dynamic.partition=true",
            "hive.exec.dynamic.partition.mode=nonstrict");
      }
      for (String query : loadDataQueries) {
        LOG.info("Load data query: {}", query);
        DefaultJDBCWrapper.executeUpdate(conn, database, query, true);
      }
    }
  }

  private Connection getConnection() {
    String url = com.hortonworks.spark.sql.hive.llap.common.HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options);
    String user = com.hortonworks.spark.sql.hive.llap.common.HWConf.USER.getFromOptionsMap(options);
    String dbcp2Configs = HWConf.DBCP2_CONF.getFromOptionsMap(options);
    return DefaultJDBCWrapper.getConnector(Option.empty(), url, user, dbcp2Configs);
  }

  @Override public void abort(WriterCommitMessage[] messages) {
    try {
      path.getFileSystem(conf).delete(path, true);
    } catch(Exception e) {
      LOG.warn("Failed to cleanup temp dir {}", path.toString());
    }
    LOG.error("Aborted DataWriter job {}", jobId);
  }

  private void logInfo(String msg) {
    LOG.info("HiveWarehouseDataSourceWriter: {}, msg:{} ", this, msg);
  }

}
