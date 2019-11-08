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

package com.hortonworks.spark.sql.hive.llap.readers;

import com.hortonworks.spark.sql.hive.llap.readers.HiveDataSourceReaderWithFilterPushDown;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * PrunedFilteredHiveDataSourceReader implements interfaces needed for pushdowns and pruning.
 *
 * 1. Spark pulls the unpruned schema -> readSchema()
 * 2. Spark pushes the pruned schema -> pruneColumns(..)
 * 3. Spark pushes the top-level filters -> pushFilters(..)
 * 4. Spark pulls the filters that are supported by datasource -> pushedFilters(..)
 * 5. Spark pulls factories, where factory/task are 1:1
 *      -> if (enableBatchRead)
 *           createBatchDataReaderFactories(..)
 *         else
 *           createDataReaderFactories(..)
 */
public class PrunedFilteredHiveDataSourceReader
        extends HiveDataSourceReaderWithFilterPushDown
        implements SupportsPushDownRequiredColumns {

  public PrunedFilteredHiveDataSourceReader(Map<String, String> options) {
    super(options);
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.schema = requiredSchema;
  }

}
