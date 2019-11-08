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

import com.hortonworks.spark.sql.hive.llap.readers.HiveWarehouseDataSourceReader;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * PrunedFilteredHiveWarehouseDataSourceReader implements interfaces needed for pushdowns and pruning.
 */
public class PrunedFilteredHiveDataSourceReader
    extends HiveWarehouseDataSourceReader
    implements SupportsPushDownRequiredColumns, SupportsPushDownFilters {

  //Pushed down filters
  //
  //"It's possible that there is no filters in the query and pushFilters(Filter[])
  // is never called, empty array should be returned for this case."
  Filter[] pushedFilters = new Filter[0];

  public PrunedFilteredHiveDataSourceReader(Map<String, String> options) throws IOException {
    super(options);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    pushedFilters = Arrays.stream(filters).
        filter((filter) -> FilterPushdown.buildFilterExpression(baseSchema, filter).isDefined()).
        toArray(Filter[]::new);

    return Arrays.stream(filters).
        filter((filter) -> !FilterPushdown.buildFilterExpression(baseSchema, filter).isDefined()).
        toArray(Filter[]::new);
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.schema = requiredSchema;
  }

  @Override
  public Filter[] getPushedFilters() {
    return pushedFilters;
  }

}