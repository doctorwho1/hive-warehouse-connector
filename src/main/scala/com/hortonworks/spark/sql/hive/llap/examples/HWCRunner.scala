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

package com.hortonworks.spark.sql.hive.llap.examples

import com.google.common.base.Preconditions
import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * To run see hwc_runner.sh
  */
object HWCRunner extends Logging {

  val hwc_examples_test_db = "hwc_examples_test_db"
  val hive_table_batch_test = "hive_table_batch_test"

  val spark: SparkSession = SparkSession.builder()
    .appName("HWCRunner")
    .getOrCreate()

  val hwc: HiveWarehouseSessionImpl = HiveWarehouseSession.session(spark).build


  def main(args: Array[String]): Unit = {

    hwc.dropDatabase(hwc_examples_test_db, true, true)
    hwc.createDatabase(hwc_examples_test_db, false)
    logInfo("Created database.." + hwc_examples_test_db)
    hwc.setDatabase(hwc_examples_test_db)
    logInfo("==========================Testing batch write==========================")
    testBatchReadAndWrite()
    logInfo("==========================Finished testing batch write==========================")

    logInfo("==========================Testing Streaming==========================")
    testStreamingWriteAndRead()
    logInfo("==========================Testing Streaming==========================")


    hwc.dropDatabase(hwc_examples_test_db, true, true)
    hwc.close()
    spark.stop
  }


  private def testBatchReadAndWrite(): Unit = {
    val numRows = 1000
    val numCols = 100
    val df = getDataFrame(numRows, numCols)
    logInfo(s"Generated dataframe ${df.schema}, writing to hive table now")

    df.write
      .format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector")
      .mode("overwrite")
      .option("table", hive_table_batch_test).save()

    logInfo("Successfully written to hive table, reading the same now...")
    val hiveDf = hwc.executeQuery(s"select * from $hive_table_batch_test")
    hiveDf.cache()
    logInfo("=======df read from hive table=========")
    hiveDf.show(false)

    val diff = df.except(hiveDf)
    if (diff.count() > 0) {
      logError("There are differences between loaded and fetched dataframe. Showing diff.")
      diff.show(numRows = numRows, truncate = false)
      throw new IllegalStateException("There are differences between loaded and fetched dataframe")
    }

    logInfo(s"================ Executing join ================")
    // this is expected to cover another path in get_splits udtf where it creates temp table
    val joinDf = hwc.executeQuery(s"select t1.* from $hive_table_batch_test t1 " +
      s" join $hive_table_batch_test t2")
    logInfo(s"======= joinDf.count = ${joinDf.count()}")
    Preconditions.checkState(joinDf.count() == numRows)
  }

  private def getDataFrame(numRows: Int, numCols: Double) = {
    val intToRow: Int => Row =
      (i: Int) => Row.fromSeq(Range.Double.inclusive(1.0, numCols, 1.0))

    val schema: StructType =
      (1 to numCols.toInt).foldLeft(new StructType())((s, i) => s.add("c" + i,
        DoubleType, nullable = true))

    val rdds = spark.sparkContext.parallelize((1 to numRows).map(intToRow))
    val df = spark.createDataFrame(rdds, schema)
    df
  }

  private def testStreamingWriteAndRead(): Unit = {

    val rateDF = spark.readStream.format("rate").option("rowsPerSecond", 1).load
    val hive_table_streaming_test = "spark_rate_source"
    hwc.executeUpdate(s"create table $hwc_examples_test_db.$hive_table_streaming_test " +
      s" (`timestamp` string, `value` bigint)" +
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", true)

    val duration_secs_conf = spark.conf.getOption("hwc.test.streaming.duration.secs")

    val min_streaming_duration_secs = 30
    var streamingQueryDuration = min_streaming_duration_secs
    if (duration_secs_conf.isDefined) {
      if (Integer.parseInt(duration_secs_conf.get) > min_streaming_duration_secs) {
        streamingQueryDuration = Integer.parseInt(duration_secs_conf.get)
      }
    }


    logInfo(s"Starting streaming query with: streamingQueryDuration = $streamingQueryDuration secs")
    logInfo(s"Using metastore uris ${spark.conf.get("spark.hadoop.hive.metastore.uris")}")
    val streamingQuery = rateDF.writeStream
      .format("com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource")
      .outputMode("append")
      .option("metastoreUri", spark.conf.get("spark.hadoop.hive.metastore.uris"))
      .option("metastoreKrbPrincipal", spark.conf.getOption("spark.hadoop.hive.metastore.kerberos.principal").orNull)
      .option("table", hive_table_streaming_test)
      .option("database", hwc_examples_test_db)
      .start()

    Thread.sleep(streamingQueryDuration * 1000)

    logInfo(s"=====Stopping streaming query...")
    streamingQuery.stop()

    val streamingSink = hwc.executeQuery(s"select * from " +
      s"$hwc_examples_test_db.$hive_table_streaming_test")

    logInfo(s"=====Records in streaming table=======")
    streamingSink.show(false)
  }

}
