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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A DataReader Implementation with loose filter pushdown rules. Disables projection pruning entirely.
 * Developed mainly due to issues where parent and child dataframes share Reader instance in spark 2.3.x
 * See https://hortonworks.jira.com/browse/BUG-118876 and https://hortonworks.jira.com/browse/BUG-121727 for more details.
 * <p>
 * In case of parent child relationship in dataframes, this performs well if some meaningful .filter() is specified in parent dataframe.
 * The stricter the definition of .filter() in parent DF, the better it will perform for both parent and child dataframes.
 * <p>
 * It takes filters from all the dataframes, joins them using 'OR' and pushes all of them to hive and hence fetches a resultset
 * inclusive of all the filters. Later more filtering is done at spark side depending on what specific filter was specified
 * on a particular dataframe.
 * This implementation is not thread safe.
 * Since this is runs in spark driver, it is assumed that spark internally never invokes
 * this code in multiple threads for the same instance of HiveDataSourceReaderForSpark23X.
 */
public class HiveDataSourceReaderForSpark23X extends HiveDataSourceReaderWithFilterPushDown {


  private static final Filter[] EMPTY_FILTER_ARRAY = new Filter[0];

  // Maintains set of all the filters that have been applied on parent->child->...dataframes.
  // While query building, these filters are OR-ed and passed to hive.
  private final Set<Set<Filter>> allFiltersToPush = new HashSet<>();

  // This variable to handle following cases:
  // 1. hive.executeQuery("select * from t1").filter("col1 > 1").show
  // For 1. sequence of operations would be pushFilters() -> getQueryString()(called from createBatchDataReaderFactories())
  // if filter is present in chain, pushFilters() will set this flag, push all the filters(joined with OR) to hive and buildWhereClauseFromFilters() resets it
  // 2. hive.executeQuery("select * from t1").show
  // For 2. pushFilters() is not invoked by spark and this flag is not set and hence no filters are pushed to hive.
  private boolean currentDFHasFilterCondition = false;

  public HiveDataSourceReaderForSpark23X(Map<String, String> options) {
    super(options);
  }

  @Override
  public Filter[] pushedFilters() {
    // Since we have OR-ed the the filters, we need spark to do specific filtering according filters of this execution.
    // this is to let spark know that we haven't pushed any filters down.
    return EMPTY_FILTER_ARRAY;
  }

  @Override
  public Filter[] getPushedFilters() {
    // Since we have OR-ed the the filters, we need spark to do specific filtering according filters of this execution.
    // this is to let spark know that we haven't pushed any filters down.
    return EMPTY_FILTER_ARRAY;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    Set<Filter> filterSet = new HashSet<>();
    for (Filter filter : filters) {
      // this means we can push this filter to hive
      if (FilterPushdown.buildFilterExpression(schema, filter).isDefined()) {
        filterSet.add(filter);
      }
    }

    if (filterSet.size() > 0) {
      currentDFHasFilterCondition = true;
      allFiltersToPush.add(filterSet);
    }

    // unsupported filters - ones which we cannot push down to hive
    // let spark know that we haven't pushed any filters to hive
    return filters;
  }

  @Override
  protected String buildWhereClauseFromFilters(Filter[] filters) {
    final String whereClause = currentDFHasFilterCondition && !allFiltersToPush.isEmpty() ?
        " WHERE " + allFiltersToPush.stream()
            .map(this::buildFilterStringWithAndJoiner)
            .collect(Collectors.joining(" OR ")) : "";
    // this flag is not used anymore in current execution, resetting it for next cycle.
    currentDFHasFilterCondition = false;
    return whereClause;
  }

  private String buildFilterStringWithAndJoiner(Set<Filter> filters) {
    return "(" + filters.stream().map(filter -> FilterPushdown.buildFilterExpression(schema, filter).get())
        .collect(Collectors.joining(" AND ")) + ")";
  }

}