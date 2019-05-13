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

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.hortonworks.spark.sql.hive.llap.SessionTestBase;
import com.hortonworks.spark.sql.hive.llap.common.Column;
import com.hortonworks.spark.sql.hive.llap.common.DescribeTableOutput;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LoadDataQueryBuilderTest extends SessionTestBase {

  private final String database = "targetDB";
  private final String table = "targetTable";
  private final String srcFilePath = "/hwc_scratch/job_staging_dir/";
  private final String jobId = "f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e";
  private StructType dfSchema = getSchema();

  private StructType getSchema() {
    return (new StructType())
        .add("id", StringType)
        .add("name", StringType)
        .add("city", StringType)
        .add("dob", StringType);
  }

  @Override
  @Before
  // Get rid of spark session being started in super, we don't need it here at present.
  public void setUp() {
  }

  @Override
  @After
  public void tearDown() {
  }

  @Test
  public void testWithoutPartitions() {
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(false, false, null);
    List<String> queries = builder.build();
    String[] expected = {
        "LOAD DATA INPATH '/hwc_scratch/job_staging_dir/' INTO TABLE targetDB.targetTable"
    };
    checkBuilderResult(queries, expected);
  }

  @Test
  public void testWithoutPartitionsWithCreateTable() {
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(true, false, null);
    List<String> queries = builder.build();
    String[] expected = {
        "CREATE TABLE  targetDB.targetTable  (id string,name string,city string,dob string)  STORED AS ORC ",
        "LOAD DATA INPATH '/hwc_scratch/job_staging_dir/' INTO TABLE targetDB.targetTable"
    };
    checkBuilderResult(queries, expected);
  }

  @Test
  public void testWithoutPartitionsWithCreateTableAndOverwrite() {
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(true, true, null);
    List<String> queries = builder.build();
    String[] expected = {
        "CREATE TABLE  targetDB.targetTable  (id string,name string,city string,dob string)  STORED AS ORC ",
        "LOAD DATA INPATH '/hwc_scratch/job_staging_dir/' OVERWRITE  INTO TABLE targetDB.targetTable"
    };
    checkBuilderResult(queries, expected);
  }

  @Test
  public void testWithStaticPartitioning() {
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(true, true, "city='BLR', dob = '2019-05-12' ");
    List<String> queries = builder.build();
    String[] expected = {
        "CREATE TABLE  targetDB.targetTable  (id string,name string)  PARTITIONED BY(city string,dob string)  STORED AS ORC ",
        "LOAD DATA INPATH '/hwc_scratch/job_staging_dir/' OVERWRITE  INTO TABLE targetDB.targetTable PARTITION (city='BLR', dob = '2019-05-12' )"
    };
    checkBuilderResult(queries, expected);
  }

  @Test
  public void testWithDynamicPartitioning() {
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(true, true, "city,dob");
    List<String> queries = builder.build();
    String[] expected = {
        "CREATE TABLE  targetDB.targetTable  (id string,name string)  PARTITIONED BY(city string,dob string)  STORED AS ORC ",
        "CREATE TEMPORARY EXTERNAL TABLE targetDB.f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e(id string,name string,city string,dob string) STORED AS ORC LOCATION '/hwc_scratch/job_staging_dir/'",
        "INSERT OVERWRITE TABLE targetDB.targetTable PARTITION (city,dob)  SELECT id,name,city,dob FROM targetDB.f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e"
    };
    checkBuilderResult(queries, expected);
  }

  @Test
  public void testWithStaticAndDynamicPartitioning() {
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(true, true, "city='BLR',dob");
    List<String> queries = builder.build();
    String[] expected = {
        "CREATE TABLE  targetDB.targetTable  (id string,name string)  PARTITIONED BY(city string,dob string)  STORED AS ORC ",
        "CREATE TEMPORARY EXTERNAL TABLE targetDB.f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e(id string,name string,city string,dob string) STORED AS ORC LOCATION '/hwc_scratch/job_staging_dir/'",
        "INSERT OVERWRITE TABLE targetDB.targetTable PARTITION (city='BLR',dob)  SELECT id,name,dob FROM targetDB.f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e"
    };
    checkBuilderResult(queries, expected);
  }

  @Test
  public void testWithStaticDynamicPartitioningWithoutOverwrite() {
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(true, false, "city='BLR',dob");
    List<String> queries = builder.build();
    String[] expected = {
        "CREATE TABLE  targetDB.targetTable  (id string,name string)  PARTITIONED BY(city string,dob string)  STORED AS ORC ",
        "CREATE TEMPORARY EXTERNAL TABLE targetDB.f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e(id string,name string,city string,dob string) STORED AS ORC LOCATION '/hwc_scratch/job_staging_dir/'",
        "INSERT INTO TABLE targetDB.targetTable PARTITION (city='BLR',dob)  SELECT id,name,dob FROM targetDB.f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e"
    };
    checkBuilderResult(queries, expected);
  }

  @Test
  public void testAllNonPartColsBeforePartColsInCreateTable() {
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(true, false, "name='a',dob='b'");
    String errMsg = "";
    try {
      builder.build();
    } catch (IllegalArgumentException e) {
      errMsg = e.getMessage();
    }
    assertTrue(errMsg.contains("Non partitioned Column: city is not allowed after partitioned column"));
  }



  @Test
  public void testBlankPartitionIsNotRespected() {
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(true, true, "    ");
    List<String> queries = builder.build();
    String[] expected = {
        "CREATE TABLE  targetDB.targetTable  (id string,name string,city string,dob string)  STORED AS ORC ",
        "LOAD DATA INPATH '/hwc_scratch/job_staging_dir/' OVERWRITE  INTO TABLE targetDB.targetTable"
    };
    checkBuilderResult(queries, expected);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPartitionSyntaxValidation() {
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(true, true, " city='BLR',,");
    builder.build();
  }

  @Test
  public void testDynamicColsVsSchemaColsOrdering() {
    StructType schema = new StructType()
        .add("c1", StringType)
        .add("c2", StringType)
        .add("c3", StringType)
        .add("c4", StringType)
        .add("c5", StringType)
        .add("c6", StringType)
        .add("c7", StringType);

    final String wrongPartitionSpec = "c3='a',c5,c4,c6,c7";
    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(schema, true, true, wrongPartitionSpec, null, false);
    String expectedMsg = "";
    try {
      builder.build();
    } catch (IllegalArgumentException e) {
      expectedMsg = e.getMessage();
    }
    assertTrue(expectedMsg.contains("Partition column c4 is not at the same position(from last) in DF schema"));

    final String rightPartitionSpec = "c3='a',c4,c5,c6,c7";
    builder = getLoadDataQueryBuilder(schema, true, true, rightPartitionSpec, null, false);

    List<String> queries = builder.build();
    String[] expected = {
        "CREATE TABLE  targetDB.targetTable  (c1 string,c2 string)  PARTITIONED BY(c3 string,c4 string,c5 string,c6 string,c7 string)  STORED AS ORC ",
        "CREATE TEMPORARY EXTERNAL TABLE targetDB.f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e(c1 string,c2 string,c3 string,c4 string,c5 string,c6 string,c7 string) STORED AS ORC LOCATION '/hwc_scratch/job_staging_dir/'",
        "INSERT OVERWRITE TABLE targetDB.targetTable PARTITION (c3='a',c4,c5,c6,c7)  SELECT c1,c2,c4,c5,c6,c7 FROM targetDB.f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e"
    };
    checkBuilderResult(queries, expected);
  }

  @Test
  public void testStaticPartWithValidateAgainstHiveColumns() {
    //c1, c2, c3
    DescribeTableOutput describeTableOutput = getDescribeTableOutput(
        Lists.newArrayList("c1", "c2", "c3"), null
    );

    StructType wrongSchema = new StructType()
        .add("c1", StringType)
        .add("c222222", StringType) //wrong column
        .add("c3", StringType)
        .add("c4", StringType)
        .add("c5", StringType);

    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(wrongSchema, false, true, null, describeTableOutput, true);
    String expectedMsg = "";
    try {
      builder.build();
    } catch (IllegalArgumentException e) {
      expectedMsg = e.getMessage();
    }
    assertTrue(expectedMsg.contains("Hive column: c2 cannot be found at same index: 1 in dataframe. Found c222222."));

    StructType rightSchema = new StructType()
        .add("c1", StringType)
        .add("c2", StringType)
        .add("c3", StringType)
        .add("c4", StringType)
        .add("c5", StringType);

    final String rightPartitionSpec = "c4='a',c5='b'";
    builder = getLoadDataQueryBuilder(rightSchema, false, true,
        rightPartitionSpec, describeTableOutput, true);

    List<String> queries = builder.build();
    // c4='a',c5='b' are overwritten even if they are present in df.
    String[] expected = {
        "LOAD DATA INPATH '/hwc_scratch/job_staging_dir/' OVERWRITE  INTO TABLE targetDB.targetTable PARTITION (c4='a',c5='b')"
    };
    checkBuilderResult(queries, expected);
  }


  @Test
  public void testPartitionColsOrderWhenNoPartSpec() {
    DescribeTableOutput describeTableOutput = getDescribeTableOutput(
        Lists.newArrayList("c1", "c2", "c3"),
        Lists.newArrayList("c4", "c5", "c6", "c7")
    );

    StructType wrongSchema = new StructType()
        .add("c1", StringType)
        .add("c2", StringType)
        .add("c3", StringType)
        .add("c444444", StringType) // wrong colname
        .add("c5", StringType)
        .add("c6", StringType)
        .add("c7", StringType);

    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(wrongSchema, false, true, null, describeTableOutput, true);
    String expectedMsg = "";
    try {
      builder.build();
    } catch (IllegalArgumentException e) {
      expectedMsg = e.getMessage();
    }
    assertTrue(expectedMsg.contains("Unable to find partition column: c4 at same position: 3"));

    StructType rightSchema = new StructType()
        .add("c1", StringType)
        .add("c2", StringType)
        .add("c3", StringType)
        .add("c4", StringType)
        .add("c5", StringType)
        .add("c6", StringType)
        .add("c7", StringType);

    builder = getLoadDataQueryBuilder(rightSchema, false, true,
        null, describeTableOutput, true);

    List<String> queries = builder.build();
    String[] expected = {
        "LOAD DATA INPATH '/hwc_scratch/job_staging_dir/' OVERWRITE  INTO TABLE targetDB.targetTable"
    };
    checkBuilderResult(queries, expected);
  }

  @Test
  public void testDynamicPartitionColsOrderSimilarToHive() {
    DescribeTableOutput describeTableOutput = getDescribeTableOutput(
        Lists.newArrayList("c1", "c2", "c3"),
        Lists.newArrayList("c4", "c5", "c6", "c7")
    );

    StructType wrongSchema = new StructType()
        .add("c1", StringType)
        .add("c2", StringType)
        .add("c3", StringType)
        .add("c5", StringType) // order of c4 and c5 changed
        .add("c4", StringType)
        .add("c6", StringType)
        .add("c7", StringType);

    final String partSpec = "c4,c5,c6,c7";

    LoadDataQueryBuilder builder = getLoadDataQueryBuilder(wrongSchema, false, true, partSpec, describeTableOutput, true);
    String expectedMsg = "";
    try {
      builder.build();
    } catch (IllegalArgumentException e) {
      expectedMsg = e.getMessage();
    }
    assertTrue(expectedMsg.contains("Unable to find partition column: c5 at same position: 2"));

    // this should pass...
    final String staticPartSpec = "c4='a', c5='b', c6, c7";
    builder = getLoadDataQueryBuilder(wrongSchema, false, true, staticPartSpec, describeTableOutput, true);
    List<String> queries = builder.build();
    // create temp table is created in the same cols order as DF which is fine
    // But in INSERT OVERWRITE, SELECT picks the right insertion order
    String[] expected = {
        "CREATE TEMPORARY EXTERNAL TABLE targetDB.f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e(c1 string,c2 string,c3 string,c5 string,c4 string,c6 string,c7 string) STORED AS ORC LOCATION '/hwc_scratch/job_staging_dir/'",
        "INSERT OVERWRITE TABLE targetDB.targetTable PARTITION (c4='a', c5='b', c6, c7)  SELECT c1,c2,c3,c6,c7 FROM targetDB.f7ad8685_b6e7_4b8c_a5b7_cdd12b6ce24e"
    };
    checkBuilderResult(queries, expected);
  }


  private void checkBuilderResult(List<String> actual, String... expectedQueries) {
    assertEquals(expectedQueries.length, actual.size());
    for (int i = 0; i < expectedQueries.length; i++) {
      assertEquals(expectedQueries[i], actual.get(i));
    }
  }


  private DescribeTableOutput getDescribeTableOutput(List<String> nonPartitionedCols, List<String> partitionedCols) {
    DescribeTableOutput describeTableOutput = new DescribeTableOutput();
    describeTableOutput.setColumns(
        nonPartitionedCols.stream().map(colName -> new Column(colName, "string"))
            .collect(Collectors.toList())
    );

    if (partitionedCols != null) {
      describeTableOutput.setPartitionedColumns(
          partitionedCols.stream().map(colName -> new Column(colName, "string"))
              .collect(Collectors.toList())
      );
    }
    return describeTableOutput;
  }


  private LoadDataQueryBuilder getLoadDataQueryBuilder(StructType dfSchema, boolean withCreateTableQuery,
                                                       boolean withOverwriteData, String withPartitionSpec,
                                                       DescribeTableOutput tableOutput,
                                                       boolean validateAgainstHiveColumns) {
    return new LoadDataQueryBuilder(database, table, srcFilePath, jobId, dfSchema)
        .withStorageFormat("ORC")
        .withCreateTableQuery(withCreateTableQuery)
        .withPartitionSpec(withPartitionSpec)
        .withOverwriteData(withOverwriteData)
        .withValidateAgainstHiveColumns(validateAgainstHiveColumns)
        .withDescribeTableOutput(tableOutput)
        ;
  }

  private LoadDataQueryBuilder getLoadDataQueryBuilder(boolean withCreateTableQuery, boolean withOverwriteData,
                                                       String withPartitionSpec) {
    return getLoadDataQueryBuilder(dfSchema, withCreateTableQuery, withOverwriteData, withPartitionSpec,
        null, false);
  }

}