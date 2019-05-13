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

package com.hortonworks.spark.sql.hive.llap.util;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * -Provides methods to map a sparkDF record({@link InternalRow}) to hive table(in the sequence of hive table columns)
 */
public class SparkToHiveRecordMapper implements Serializable {

  private static final long serialVersionUID = -5213255591548912610L;
  private static final Logger LOG = LoggerFactory.getLogger(SparkToHiveRecordMapper.class);

  private StructType schemaInHiveColumnsOrder;
  private int[] mapping;
  private boolean recordReorderingNeeded = false;
  private StructType sparkDFSchema;

  public SparkToHiveRecordMapper(StructType sparkDFSchema, String[] hiveTableColumns) {
    this.sparkDFSchema = sparkDFSchema;
    this.schemaInHiveColumnsOrder = sparkDFSchema;
    resolveBasedOnColumnNames(hiveTableColumns);
  }

  public SparkToHiveRecordMapper() {
  }

  public StructType getSchemaInHiveColumnsOrder() {
    return schemaInHiveColumnsOrder;
  }

  /**
   * Permutes fields of record according to hive column sequence if needed.
   *
   * @param record record
   * @return record having same sequence to that of hive column sequence
   */
  public InternalRow mapToHiveColumns(InternalRow record) {
    if (this.recordReorderingNeeded) {
      int numFields = record.numFields();
      Object[] reordered = new Object[numFields];
      for (int i = 0; i < numFields; i++) {
        reordered[mapping[i]] = record.get(i, sparkDFSchema.fields()[i].dataType());
      }
      record = new GenericInternalRow(reordered);
    }
    return record;
  }

  /**
   * Builds mapping between sparkDF columns to hive columns and sets recordReorderingNeeded flag if
   * all the column names of dataframe are present in hive table and their order is different.
   *
   * It does not set recordReorderingNeeded flag and does not perform any schema reordering if
   * <br/>
   *  - Hive columns and dataframe columns differ in length
   * <br/>
   *  - Or names of all the columns in DF are not similar to those in hive
   *
   * <p>
   * <br/>
   * Eg: If mapping is [3,2,0,1], this implies that
   * <br/>Spark DF column ----corresponds to -----> Hive column
   * <br/>    0                                      3
   * <br/>    1                                      2
   * <br/>    2                                      0
   * <br/>    3                                      1
   */
  private void resolveBasedOnColumnNames(String[] hiveTableColumns) {
    String[] dataframeColumns = sparkDFSchema.fieldNames();
    if (hiveTableColumns != null && hiveTableColumns.length == dataframeColumns.length) {
      Map<String, Integer> hiveColumnIndexMapping = IntStream.range(0, hiveTableColumns.length)
          .boxed()
          .collect(Collectors.toMap(i -> hiveTableColumns[i], Function.identity()));

      this.mapping = new int[hiveTableColumns.length];
      Integer hiveColIndex;
      for (int i = 0; i < dataframeColumns.length; i++) {
        //some column in dataframe is not in hive, we cannot go for reordering, hence reset recordReorderingNeeded
        if ((hiveColIndex = hiveColumnIndexMapping.get(dataframeColumns[i])) == null) {
          this.recordReorderingNeeded = false;
          break;
        }
        this.mapping[i] = hiveColIndex;
        //i.e. sequence of columns in df is different to that of hive columns, we will need to reorder
        if (i != hiveColIndex) {
          this.recordReorderingNeeded = true;
        }
      }
      //if reordering is needed, reorder schema as well...as later it is passed to OrcOutputWriter
      if (this.recordReorderingNeeded) {
        this.schemaInHiveColumnsOrder = new StructType();
        for (String hiveTableColumn : hiveTableColumns) {
          this.schemaInHiveColumnsOrder = this.schemaInHiveColumnsOrder.add(sparkDFSchema.fields()[sparkDFSchema.fieldIndex(hiveTableColumn)]);
        }
      }
    }
    LOG.info("Need to reorder/rearrange DF columns to match hive columns order: {}", this.recordReorderingNeeded);
  }
}
