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

import org.apache.spark.sql.sources.Filter;

import java.util.Map;

/**
 * A DataReader Implementation which only supports pushdowns. Disables projection pruning entirely.
 * Developed mainly due to issues where parent and child dataframes share Reader instance in spark 2.3.x
 * See https://hortonworks.jira.com/browse/BUG-118876 and https://hortonworks.jira.com/browse/BUG-121727 for more details.
 * <p>
 * <p>
 * This implementation is not thread safe.
 * Since this is runs in spark driver, it is assumed that spark internally never invokes
 * this code in multiple threads for the same instance of HiveDataSourceReaderForSpark23X.
 */
public class HiveDataSourceReaderForSpark23X extends HiveDataSourceReaderWithFilterPushDown {

  /**
   * this flag is set when pushFilters() is invoked by spark, which indicates that we have presence of filters somewhere in the df lineage.
   */
  private boolean currentDFHasFilterCondition = false;

  public HiveDataSourceReaderForSpark23X(Map<String, String> options) {
    super(options);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    currentDFHasFilterCondition = true;
    return super.pushFilters(filters);
  }

  @Override
  protected String buildWhereClauseFromFilters(Filter[] filters) {
    final String whereClause = currentDFHasFilterCondition && filters.length > 0 ?
        super.buildWhereClauseFromFilters(filters) : "";
    // this flag is not used anymore in current execution, resetting it for next cycle.
    currentDFHasFilterCondition = false;
    return whereClause;
  }
}