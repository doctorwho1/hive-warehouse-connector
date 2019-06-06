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

package com.hortonworks.spark.sql.hive.llap

import com.hortonworks.spark.sql.hive.llap.query.builder.LoadDataQueryBuilderTest
import org.scalatest.FunSuite

class TestJavaProxy extends FunSuite {

  def withSetUpAndTearDown(suite: SessionTestBase, test: () => Unit): Unit = try {
    suite.setUp()
      test()
    } finally {
    suite.tearDown()
    }

  test("HiveWarehouseBuilderTest") {
    val test = new HiveWarehouseBuilderTest()
    withSetUpAndTearDown(test, test.testNewEntryPoint)
    withSetUpAndTearDown(test, test.testAllBuilderConfig)
    withSetUpAndTearDown(test, test.testAllConfConfig)
  }

  test("HiveWarehouseSessionHiveQlTest") {
    val test = new HiveWarehouseSessionHiveQlTest()
    withSetUpAndTearDown(test, test.testCreateDatabase)
    withSetUpAndTearDown(test, test.testCreateTable)
    withSetUpAndTearDown(test, test.testCreateTableWithPropagateException)
    withSetUpAndTearDown(test, test.testDescribeTable)
    withSetUpAndTearDown(test, test.testExecuteQuery)
    withSetUpAndTearDown(test, test.testUnqualifiedTable)
    withSetUpAndTearDown(test, test.testQualifiedTable)
    withSetUpAndTearDown(test, test.testSetDatabase)
    withSetUpAndTearDown(test, test.testShowTable)
    withSetUpAndTearDown(test, test.testMergeBuilder)
    withSetUpAndTearDown(test, test.testMergeBuilderWithSourceExpression)
    withSetUpAndTearDown(test, test.testMergeBuilderWithoutMatchExpressions)
  }

  test("TestSecureHS2Url") {
    val test = new TestSecureHS2Url()
    withSetUpAndTearDown(test, test.kerberizedClusterMode)
    withSetUpAndTearDown(test, test.kerberizedClientMode)
    withSetUpAndTearDown(test, test.nonKerberized)
  }

  test("TestWriteSupport") {
    val test = new TestWriteSupport()
    withSetUpAndTearDown(test, test.testWriteSupport);
  }

  test("SchemaUtilTest") {
    val test = new SchemaUtilTest()
    withSetUpAndTearDown(test, test.testBuildHiveCreateTableQueryFromSparkDFSchema)
  }

  test("HiveQlUtilTest") {
    val test = new HiveQlUtilTest()
    withSetUpAndTearDown(test, test.testFormatRecord)
    withSetUpAndTearDown(test, test.testTimestampSchemaUnsafeRow)
  }

  test("TestReadSupport") {
    val test = new TestReadSupport()
    withSetUpAndTearDown(test, test.testReadSupport);
    withSetUpAndTearDown(test, test.testCountSupport);
    withSetUpAndTearDown(test, test.testSmartExecutionCases);
  }

  test("SparkToHiveRecordMapperTest") {
    val test = new SparkToHiveRecordMapperTest()
    withSetUpAndTearDown(test, test.testMapToHiveColumns)
    withSetUpAndTearDown(test, test.testMapToHiveColumnsWithRightSequence)
    withSetUpAndTearDown(test, test.testMapToHiveColumnsWithHiveColumnsNull)
    withSetUpAndTearDown(test, test.testWithDifferentColumnsInDF)
    withSetUpAndTearDown(test, test.testWithDifferentNumberOfColsInHiveAndDF)
  }

  test("StreamingRecordFormatterTest") {
    val test = new StreamingRecordFormatterTest()
    withSetUpAndTearDown(test, test.testSimpleTypes)
    withSetUpAndTearDown(test, test.testStructInArray)
    withSetUpAndTearDown(test, test.testStructInArrayMultiRowsAndCols)
    withSetUpAndTearDown(test, test.testArrayInsideStruct)
    withSetUpAndTearDown(test, test.testTimestampSchemaUnsafeRow)
  }

  test("LoadDataQueryBuilderTest") {
    val test = new LoadDataQueryBuilderTest()
    withSetUpAndTearDown(test, test.testWithoutPartitions)
    withSetUpAndTearDown(test, test.testWithoutPartitionsWithCreateTable)
    withSetUpAndTearDown(test, test.testWithoutPartitionsWithCreateTableAndOverwrite)
    withSetUpAndTearDown(test, test.testWithStaticPartitioning)
    withSetUpAndTearDown(test, test.testWithDynamicPartitioning)
    withSetUpAndTearDown(test, test.testWithStaticAndDynamicPartitioning)
    withSetUpAndTearDown(test, test.testWithStaticDynamicPartitioningWithoutOverwrite)
    withSetUpAndTearDown(test, test.testAllNonPartColsBeforePartColsInCreateTable)
    withSetUpAndTearDown(test, test.testBlankPartitionIsNotRespected)
    val thrown = intercept[IllegalArgumentException] {
      test.testPartitionSyntaxValidation()
    }
    assert(thrown.getMessage.startsWith("Invalid partition spec:"))
    withSetUpAndTearDown(test, test.testDynamicColsVsSchemaColsOrdering)
    withSetUpAndTearDown(test, test.testStaticPartWithValidateAgainstHiveColumns)
    withSetUpAndTearDown(test, test.testPartitionColsOrderWhenNoPartSpec)
    withSetUpAndTearDown(test, test.testDynamicPartitionColsOrderSimilarToHive)
  }

}
