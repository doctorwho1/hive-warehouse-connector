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

package com.hortonworks.spark.sql.hive.llap;

import java.sql.Connection;
import java.util.Map;

import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import com.hortonworks.spark.sql.hive.llap.util.SerializableHadoopConfiguration;
import com.hortonworks.spark.sql.hive.llap.util.SparkToHiveRecordMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.loadInto;

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
 * 2.1) Specified table exists in hive and specified savemode is not {@link SaveMode#Overwrite}
 * <br/>
 * 2.2) All the column names in dataframe are identical to those of hive table.
 * <br/>
 * 2.3) Order of columns in dataframe is different to that of in hive table.
 */
public class HiveWarehouseDataSourceWriter implements DataSourceWriter {
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

  public HiveWarehouseDataSourceWriter(Map<String, String> options, String jobId, StructType schema,
                                       Path path, Configuration conf, SaveMode mode) {
    this.options = options;
    this.jobId = jobId;
    this.schema = schema;
    this.mode = mode;
    this.path = new Path(path, jobId);
    this.conf = conf;
    populateDBTableNames(options.get("database"));
  }

  private void populateDBTableNames(String optionDatabase) {
    String database = optionDatabase;
    if(database == null) {
      database = HWConf.DEFAULT_DB.getFromOptionsMap(options);
    }
    String table = options.get("table");
    SchemaUtil.TableRef tableRef = SchemaUtil.getDbTableNames(database, table);
    this.tableName = tableRef.tableName;
    this.databaseName = tableRef.databaseName;
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    String hiveCols[] = null;
    try (Connection connection = getConnection()) {
      this.tableExists = DefaultJDBCWrapper.tableExists(connection, databaseName, tableName);
      //for SaveMode.Overwrite, existing hiveCols does not matter as table will be dropped anyway
      if (this.tableExists && !SaveMode.Overwrite.equals(mode)) {
        hiveCols = DefaultJDBCWrapper.getTableColumns(connection, databaseName, tableName);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    SparkToHiveRecordMapper sparkToHiveRecordMapper = new SparkToHiveRecordMapper(schema, hiveCols);
    return new HiveWarehouseDataWriterFactory(jobId, schema, path, new SerializableHadoopConfiguration(conf), sparkToHiveRecordMapper);
  }

  //uses output coordinator to ensure atmost one task commit for a partition
  @Override
  public boolean useCommitCoordinator() {
    return true;
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {
    //nothing to do/clean up as of now
  }


  @Override public void commit(WriterCommitMessage[] messages) {
    try {
      String url = HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options);
      String user = HWConf.USER.getFromOptionsMap(options);
      String dbcp2Configs = HWConf.DBCP2_CONF.getFromOptionsMap(options);
      String database = HWConf.DEFAULT_DB.getFromOptionsMap(options);
      String table = options.get("table");
      SchemaUtil.TableRef tableRef = SchemaUtil.getDbTableNames(database, table);
      database = tableRef.databaseName;
      table = tableRef.tableName;
      try (Connection conn = DefaultJDBCWrapper.getConnector(Option.empty(), url, user, dbcp2Configs)) {
        handleWriteWithSaveMode(database, table, conn);
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

  private void handleWriteWithSaveMode(String database, String table, Connection conn) {
    boolean createTable = false;
    boolean loadData = false;
    String truncateOption = options.get(HWConf.TRUNCATE_OPTION_KEY);
    switch (mode) {
      case ErrorIfExists:
        if (tableExists) {
          throw new IllegalArgumentException("Table[" + table + "] already exists, please specify a different SaveMode");
        }
        createTable = true;
        loadData = true;
        break;
      case Append:
        //create if table does not exist
        //https://lists.apache.org/thread.html/40e6630f608802b31e6fdc537cf3ddbc07fe2d48c2ad51df91ae6e67@%3Cdev.spark.apache.org%3E
        if (!tableExists) {
          createTable = true;
        }
        loadData = true;
        break;
      case Overwrite:
        if (tableExists) {
          //option("truncate", "true")
          if ("true".equalsIgnoreCase(truncateOption)) {
            DefaultJDBCWrapper.truncateTable(conn, database, table);
          } else {
            DefaultJDBCWrapper.dropTable(conn, database, table, false);
            createTable = true;
          }
        } else {
          createTable = true;
        }
        loadData = true;
        break;
      case Ignore:
        //NO-OP if table already exists
        if (!tableExists) {
          createTable = true;
          loadData = true;
        }
        break;
    }

    LOG.info("Handling write: database:{}, table:{}, savemode: {}, tableExists:{}, createTable:{}, loadData:{}, truncate:{}",
        database, table, mode, tableExists, createTable, loadData, truncateOption);
    if (createTable) {
      String createTableQuery = SchemaUtil.buildHiveCreateTableQueryFromSparkDFSchema(schema, database, table);
      DefaultJDBCWrapper.executeUpdate(conn, database, createTableQuery, true);
    }

    if (loadData) {
      DefaultJDBCWrapper.executeUpdate(conn, database, loadInto(this.path.toString(), database, table), true);
    }
  }

  private Connection getConnection() {
    String url = HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options);
    String user = HWConf.USER.getFromOptionsMap(options);
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

}
