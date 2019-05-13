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

package com.hortonworks.spark.sql.hive.llap.query.builder;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.hortonworks.spark.sql.hive.llap.CreateTableBuilder;
import com.hortonworks.spark.sql.hive.llap.common.Column;
import com.hortonworks.spark.sql.hive.llap.common.DescribeTableOutput;
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.loadInto;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.wrapWithSingleQuotes;
import static com.hortonworks.spark.sql.hive.llap.util.SchemaUtil.getHiveType;

/**
 * Builds queries for loading data. Following are the rules for query building:-
 * <p>
 * 1) Partitioning Rules
 * <p>
 * 1.1) Without partition: i.e. df.write.format("...HiveWarehouseConnector").mode("<mode>").option("table", "t1").save
 * With no partition information specified, it simply generates a LOAD DATA query.
 * E.g. LOAD DATA INPATH '<load.staging.dir>' [OVERWRITE] INTO TABLE db.t1
 * <p>
 * 1.2) With static partitions only: i.e. df.write.format("...HiveWarehouseConnector").mode("<mode>")
 * .option("partition", "c1='val1',c2='val2'").option("table", "t1").save
 * When only static partitions are specified, it generates a LOAD DATA...PARTITION query.
 * E.g. LOAD DATA INPATH '<load.staging.dir>' [OVERWRITE] INTO TABLE db.t1 PARTITION (c1='val1',c2='val2')
 * <p>
 * 1.3) With dynamic partitions: i.e. df.write.format("...HiveWarehouseConnector").mode("<mode>")
 * .option("partition", "c1='val1',c2").option("table", "t1").save
 * When atleast one dynamic partition is present, it generate 2 queries, CREATE TEMP EXTERNAL TABLE.. and INSERT INTO/OVERWRITE...SELECT cols FROM...
 * E.g. CREATE TEMPORARY EXTERNAL TABLE db.job_id_table(cols....) STORED AS ORC LOCATION '<load.staging.dir>'
 * INSERT INTO/OVERWRITE TABLE t1 PARTITION (c1='val1',c2)  SELECT cols_excluding_static_partitioning_cols FROM db.job_id_table
 * <p>
 * 2) createTable flag: Also generates a CREATE TABLE query for target table when this flag is true.
 * <p>
 * 3) overwriteData flag: Used to decide between INTO and OVERWRITE in above queries.
 * <p>
 * NOTE: This expects user to pass in, the right order of columns in dataframe and partition spec according to the actual hive
 * table definition to work correctly.
 *
 * @see com.hortonworks.spark.sql.hive.llap.util.SparkToHiveRecordMapper
 */
public class LoadDataQueryBuilder {

  private final String database;
  private final String table;
  private final String srcFilePath;
  private final String jobId;
  private final StructType dfSchema;


  private boolean overwriteData = false;
  private String partitionSpec;
  private String storageFormat = "ORC";
  private boolean validateAgainstHiveColumns;

  private boolean dynamicPartitionPresent = false;
  private LinkedHashMap<String, String> partitions;
  private boolean createTable = false;
  private DescribeTableOutput describeTableOutput;

  public LoadDataQueryBuilder(String database, String table, String srcFilePath, String jobId, StructType dfSchema) {
    this.database = database;
    this.table = table;
    this.srcFilePath = srcFilePath;
    this.jobId = jobId;
    this.dfSchema = dfSchema;
  }

  public LoadDataQueryBuilder withCreateTableQuery(boolean createTable) {
    this.createTable = createTable;
    return this;
  }

  public LoadDataQueryBuilder withOverwriteData(boolean overwriteData) {
    this.overwriteData = overwriteData;
    return this;
  }

  public LoadDataQueryBuilder withPartitionSpec(String partitionSpec) {
    this.partitionSpec = partitionSpec;
    return this;
  }

  public LoadDataQueryBuilder withStorageFormat(String storageFormat) {
    this.storageFormat = storageFormat;
    return this;
  }

  public LoadDataQueryBuilder withValidateAgainstHiveColumns(boolean validateAgainstHiveColumns) {
    this.validateAgainstHiveColumns = validateAgainstHiveColumns;
    return this;
  }

  public LoadDataQueryBuilder withDescribeTableOutput(DescribeTableOutput describeTableOutput) {
    this.describeTableOutput = describeTableOutput;
    return this;
  }

  public List<String> build() {
    parsePartitionSpec();
    List<String> queries = new ArrayList<>();
    if (validateAgainstHiveColumns) {
      Preconditions.checkState(describeTableOutput != null,
          "describeTableOutput cannot be null when validateAgainstHiveColumns is true");
      ensureDFNonPartColsAtSamePositionAsHiveNonPartCols();
      ensureDFColumnsOrderAgainstHivePartitionCols();
    }

    if (createTable) {
      ensureAllPartSpecDynamicPartColsAtTheEndOfDFSchema();
      queries.add(buildCreateTableQuery());
    }
    if (dynamicPartitionPresent) {
      queries.add(createTempTableQuery());
      queries.add(insertOverwriteQuery());
    } else {
      queries.add(loadInto(srcFilePath, database, table, overwriteData, isPartitionPresent() ? partitionSpec : null));
    }
    return queries;
  }

  // this ensures user doesn't accidentally miss a non partitioned column in dataframe
  // we don't need to validate partitioned columns here(against hiveCols) as the general check
  // ensureDFColumnsOrderAgainstHivePartitionCols() will do that.
  // Once above conditions are met, we don't need to care if static partition columns are present somewhere in between or not.
  private void ensureDFNonPartColsAtSamePositionAsHiveNonPartCols() {
    List<Column> nonPartitionedHiveCols = describeTableOutput.getColumns();
    String[] dfCols = dfSchema.fieldNames();
    String errMsg = String.format("Number of columns in dataframe:%s should be >= no of non partitioned columns in hive:" +
        " %s", dfCols.length, nonPartitionedHiveCols.size());
    Preconditions.checkArgument(nonPartitionedHiveCols.size() <= dfCols.length, errMsg);
    String colMismatchMsg = "Hive column: %s cannot be found at same index: %s in dataframe. Found %s." +
        " Aborting as this may lead to loading of incorrect data.";
    for (int i = 0; i < nonPartitionedHiveCols.size(); i++) {
      String hiveCol = nonPartitionedHiveCols.get(i).getName();
      Preconditions.checkArgument(hiveCol.equalsIgnoreCase(dfCols[i]), String.format(
          colMismatchMsg, hiveCol, i, dfCols[i]));
    }
  }


  private void ensureDFColumnsOrderAgainstHivePartitionCols() {
    // if no partSpec is specified, it checks that all the partitioned columns in df are in a order similar to hive
    if (CollectionUtils.isNotEmpty(describeTableOutput.getPartitionedColumns()) && partitions == null) {
      //predicate will always return true which means always match the column names
      ensureDFColumnsOrderAgainstHivePartitionCols(col -> true);
    } else if (dynamicPartitionPresent) {
      //if partSpec is specified, check for order of dynamic cols only
      ensureDFColumnsOrderAgainstHivePartitionCols(this::isDynamicPartitionCol);
    }
  }

  // traverses partitioned columns in hive from last to first
  // matches corresponding element from last in df schema if matchColumnNamesIfHiveColumnIs evaluates to true
  private void ensureDFColumnsOrderAgainstHivePartitionCols(Predicate<String> matchColumnNamesIfHiveColumnIs) {
    final String[] dfCols = dfSchema.fieldNames();
    final int numSchemaCols = dfCols.length;
    final List<Column> partColsFromHive = describeTableOutput.getPartitionedColumns();
    final int numHivePartCols = partColsFromHive.size();

    String schemaColsGTPartCols = String.format("Number of columns in dataframe: %s should be greater than partitioned" +
        " columns in hive table definition: %s", Arrays.toString(dfCols), partColsFromHive);
    Preconditions.checkArgument(numSchemaCols > numHivePartCols, schemaColsGTPartCols);

    for (int i = numHivePartCols - 1, j = numSchemaCols - 1; i >= 0; i--, j--) {
      String hivePartCol = partColsFromHive.get(i).getName();
      if (matchColumnNamesIfHiveColumnIs.test(hivePartCol)) {
        String schemaCol = dfCols[j];
        String errorMsg = String.format("Order of dynamic partition columns in schema must match to that " +
                "given in Hive table definition or if no partSpec is specified " +
                " then order of all partition cols should match. " +
                " Dataframe schema: %s, Partitioned Cols in Hive: %s. " +
                "Unable to find partition column: %s at same position: %s (from last) in dataframe schema",
            Arrays.toString(dfCols), partColsFromHive, hivePartCol, (numHivePartCols - i - 1));
        Preconditions.checkArgument(hivePartCol.equalsIgnoreCase(schemaCol), errorMsg);
      }
    }
  }

  private void ensureAllPartSpecDynamicPartColsAtTheEndOfDFSchema() {
    if (dynamicPartitionPresent) {
      //keyset will be ordered here
      List<String> partitions = new ArrayList<>(this.partitions.keySet());
      String[] dfCols = dfSchema.fieldNames();
      int numSchemaCols = dfCols.length;
      int numPartCols = partitions.size();
      String invalidColsLengthErrMsg = String.format("Number of columns in dataframe should be >" +
              " number of partition columns in partition Spec. Dataframe cols: %s, Partition cols: %s",
          Arrays.toString(dfCols), partitions);
      Preconditions.checkArgument(numPartCols < numSchemaCols, invalidColsLengthErrMsg);

      for (int i = numPartCols - 1, j = numSchemaCols - 1; i >= 0; i--) {
        String partCol = partitions.get(i);
        if (isDynamicPartitionCol(partCol)) {
          String schemaCol = dfCols[j];
          String errorMsg = String.format("When creating table, order of dynamic partition columns in df " +
                  "schema and partitionSpec must match. Dataframe schema: %s, Dynamic partition columns: %s." +
                  " Partition column %s is not at the same position(from last) in DF schema.",
              Arrays.toString(dfCols), partitions, partCol);
          Preconditions.checkArgument(partCol.equalsIgnoreCase(schemaCol), errorMsg);
          j--;
        }
      }
    }
  }


  private String buildCreateTableQuery() {
    CreateTableBuilder builder = new CreateTableBuilder(null, database, table);
    boolean partitionColumnSeen = false;
    // add all non-partitioned columns first
    for (StructField field : dfSchema.fields()) {
      if (!isPartitionCol(field.name())) {
        // we have encountered a non-partitioned column after a partitioned one. Not allowed!!!
        if (partitionColumnSeen) {
          String msg = String.format(
              "Non partitioned Column: %s is not allowed after partitioned column. " +
                  "Please ensure to place partitioned columns after non-partitioned ones.", field.name()
          );
          throw new IllegalArgumentException(msg);
        }
        builder.column(field.name(), getHiveType(field.dataType(), field.metadata()));
      } else {
        partitionColumnSeen = true;
      }
    }
    // now add columns from partitionSpec in order to add all the partition cols(both static and dynamic) in definition
    if (partitions != null) {
      for (Map.Entry<String, String> part : partitions.entrySet()) {
        StructField field = dfSchema.fields()[(dfSchema.fieldIndex(part.getKey()))];
        String hiveType = getHiveType(field.dataType(), field.metadata());
        builder.partition(field.name(), hiveType);
      }
    }
    return builder.toString();
  }

  private boolean isPartitionPresent() {
    return partitions != null && partitions.size() > 0;
  }

  public boolean isDynamicPartitionPresent() {
    return dynamicPartitionPresent;
  }

  private boolean isPartitionCol(String columnName) {
    return partitions != null && partitions.containsKey(columnName);
  }

  private boolean isStaticPartitionCol(String columnName) {
    return isPartitionCol(columnName) && partitions.get(columnName) != null;
  }

  private boolean isDynamicPartitionCol(String columnName) {
    return isPartitionCol(columnName) && partitions.get(columnName) == null;
  }

  private String insertOverwriteQuery() {
    StringBuilder builder = new StringBuilder("INSERT ")
        .append(overwriteData ? "OVERWRITE" : "INTO").append(" TABLE ")
        .append(database).append(".").append(table)
        .append(" PARTITION (").append(partitionSpec).append(") ");

    // remove all static partitions from select(hive in insert-overwrite throws number of columns mismatch)
    // keep the dynamic partitions, assuming that user has given them in right order(i.e. order of table columns vs order of dynamic partitions) in DF
    // It is expected that dynamic partition column values are placed at the end after table columns in the data file.
    // Also, it should be in the order of partitions defined while creating the table.
    builder.append(" SELECT ");

    String colsWithoutStaticPartitions = Arrays.stream(dfSchema.fields())
        .map(StructField::name)
        .filter(colName -> !isStaticPartitionCol(colName))
        .collect(Collectors.joining(","));

    builder.append(colsWithoutStaticPartitions)
        .append(" FROM ").append(getTempTableName());

    return builder.toString();
  }

  private String createTempTableQuery() {
    StringBuilder builder = new StringBuilder("CREATE TEMPORARY EXTERNAL TABLE ")
        .append(getTempTableName())
        .append("(");

    String columns = Arrays.stream(dfSchema.fields())
        .map(f -> f.name() + " " + getHiveType(f.dataType(), f.metadata()))
        .collect(Collectors.joining(","));
    builder.append(columns);

    builder.append(") STORED AS ").append(storageFormat)
        .append(" LOCATION ").append(wrapWithSingleQuotes(srcFilePath));

    return builder.toString();
  }

  private String getTempTableName() {
    return database + "." + HiveQlUtil.getCleanSqlAlias(jobId);
  }

  private void parsePartitionSpec() {
    if (StringUtils.isNotBlank(partitionSpec)) {
      Tree partSpecNode = getPartitionSpecNode();
      // partSpecNode (TOK_PARTSPEC) have children of kind TOK_PARTVAL which signify actual partition column names and values
      this.partitions = new LinkedHashMap<>(partSpecNode.getChildCount());
      for (int i = 0; i < partSpecNode.getChildCount(); i++) {
        Tree partValNode = partSpecNode.getChild(i);
        if (partValNode.getChildCount() == 2) {
          partitions.put(partValNode.getChild(0).getText(), partValNode.getChild(1).getText());
        } else {
          this.dynamicPartitionPresent = true;
          partitions.put(partValNode.getChild(0).getText(), null);
        }
      }
    }
  }

  private Tree getPartitionSpecNode() {
    //with this dummy query, let's parse partition with hive parser.
    String dummyQuery = String.format("INSERT INTO TABLE t1 PARTITION (%s) SELECT 1", partitionSpec);
    ASTNode ast;
    try {
      ast = ParseUtils.parse(dummyQuery);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Invalid partition spec: " + partitionSpec, e);
    }
    // In our dummyQuery above with partitionSpec, we find TOK_PARTSPEC at fourth level in the parse tree
    // i.e. TOK_QUERY -> TOK_INSERT -> TOK_INSERT_INTO -> TOK_TAB -> TOK_PARTSPEC
    return ast.getChild(0).getChild(0).getChild(0).getChild(1);
  }

}
