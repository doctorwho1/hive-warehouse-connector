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

package com.hortonworks.spark.sql.hive.llap.common;

import java.util.List;
import java.util.Optional;

/**
 * DescribeTableOutput - To hold output of desc formatted <table> command
 */
public class DescribeTableOutput {

  private List<Column> columns;
  private List<Column> partitionedColumns;
  private List<Column> detailedTableInfo;
  private List<Column> storageInfo;

  //Add more as needed

  public List<Column> getColumns() {
    return columns;
  }

  public void setColumns(List<Column> columns) {
    this.columns = columns;
  }

  public List<Column> getPartitionedColumns() {
    return partitionedColumns;
  }

  public void setPartitionedColumns(List<Column> partitionedColumns) {
    this.partitionedColumns = partitionedColumns;
  }

  public List<Column> getDetailedTableInfo() {
    return detailedTableInfo;
  }

  public void setDetailedTableInfo(List<Column> detailedTableInfo) {
    this.detailedTableInfo = detailedTableInfo;
  }

  public List<Column> getStorageInfo() {
    return storageInfo;
  }

  public void setStorageInfo(List<Column> storageInfo) {
    this.storageInfo = storageInfo;
  }

  public Column findByColNameInStorageInfo(String columnName, boolean errorOnUnsuccessfulSearch) {
    return findByColName(storageInfo, columnName, errorOnUnsuccessfulSearch);
  }

  private Column findByColName(List<Column> columns, String columnName, boolean errorOnUnsuccessfulSearch) {
    Optional<Column> column = columns.stream().filter(col -> col.getName().equalsIgnoreCase(columnName)).findFirst();
    if (column.isPresent()) {
      return column.get();
    } else {
      if (errorOnUnsuccessfulSearch) {
        throw new IllegalArgumentException("Column with name: "
            + columnName + " cannot be found in describe table output");
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "DescribeTableOutput{" +
        "columns=" + columns +
        ", partitionedColumns=" + partitionedColumns +
        ", detailedTableInfo=" + detailedTableInfo +
        ", storageInfo=" + storageInfo +
        '}';
  }
}
