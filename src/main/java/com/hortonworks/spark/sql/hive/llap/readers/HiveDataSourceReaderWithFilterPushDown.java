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

import com.hortonworks.spark.sql.hive.llap.FilterPushdown;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;

import java.util.Arrays;
import java.util.Map;

/**
 * 1. Spark pulls the unpruned schema -> readSchema()
 * 2. Spark pushes the top-level filters -> pushFilters(..)
 * 3. Spark pulls the filters that are supported by datasource -> pushedFilters(..)
 * 4. Spark pulls factories, where factory/task are 1:1
 *          -> if (enableBatchRead)
 *               createBatchDataReaderFactories(..)
 *             else
 *               createDataReaderFactories(..)
 */
public class HiveDataSourceReaderWithFilterPushDown
        extends HiveWarehouseDataSourceReader
        implements SupportsPushDownFilters {

  //Pushed down filters
  //
  //"It's possible that there is no filters in the query and pushFilters(Filter[])
  // is never called, empty array should be returned for this case."
  protected Filter[] pushedFilters = new Filter[0];

  public HiveDataSourceReaderWithFilterPushDown(Map<String, String> options) {
    super(options);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    pushedFilters = Arrays.stream(filters).
        filter((filter) -> FilterPushdown.buildFilterExpression(baseSchema, filter).isDefined()).
        toArray(Filter[]::new);

    // unsupported filters - ones which we cannot push down to hive
    return Arrays.stream(filters).
        filter((filter) -> !FilterPushdown.buildFilterExpression(baseSchema, filter).isDefined()).
        toArray(Filter[]::new);
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }


  @Override
  public Filter[] getPushedFilters() {
    return pushedFilters;
  }

}
