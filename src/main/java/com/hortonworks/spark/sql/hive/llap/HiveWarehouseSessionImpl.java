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

import com.google.common.base.Preconditions;
import com.hortonworks.hwc.MergeBuilder;
import com.hortonworks.spark.sql.hive.llap.util.FunctionWith4Args;
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.ExecutionMethod;
import com.hortonworks.spark.sql.hive.llap.util.StreamingMetaCleaner;
import com.hortonworks.spark.sql.hive.llap.util.TriFunction;
import org.apache.commons.lang.BooleanUtils;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.hortonworks.spark.sql.hive.llap.HWConf.DEFAULT_DB;
import static com.hortonworks.spark.sql.hive.llap.HWConf.MAX_EXEC_RESULTS;
import static com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.ExecutionMethod.EXECUTE_HIVE_JDBC;
import static com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.ExecutionMethod.EXECUTE_QUERY_LLAP;
import static com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.resolveExecutionMethod;

public class HiveWarehouseSessionImpl extends com.hortonworks.hwc.HiveWarehouseSession {

  private static final Logger LOG = LoggerFactory.getLogger(HiveWarehouseSessionImpl.class);

  public static final String SESSION_CLOSED_MSG = "Session is closed!!! No more operations can be performed. Please create a new HiveWarehouseSession";
  public static final String HWC_SESSION_ID_KEY = "hwc_session_id";
  static String HIVE_WAREHOUSE_CONNECTOR_INTERNAL = HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR;

  protected HiveWarehouseSessionState sessionState;

  protected Supplier<Connection> getConnector;

  protected TriFunction<Connection, String, String, DriverResultSet> executeStmt;

  protected TriFunction<Connection, String, String, Boolean> executeUpdate;

  protected FunctionWith4Args<Connection, String, String, Boolean, Boolean> executeUpdateWithPropagateException;

  /**
   * Keeps resources handles by session id. As of now these resources handles are llap handles which are basically
   * jdbc connections, locks etc.
   * {@link #close()} method closes all of them and session as well.
   */
  private static final Map<String, Set<String>> RESOURCE_IDS_BY_SESSION_ID = new HashMap<>();

  private final String sessionId;
  private final AtomicReference<HwcSessionState> hwcSessionStateRef;

  public HiveWarehouseSessionImpl(HiveWarehouseSessionState sessionState) {
    this.sessionState = sessionState;
    this.sessionId = UUID.randomUUID().toString();
    getConnector = () -> DefaultJDBCWrapper.getConnector(sessionState);
    executeStmt = (conn, database, sql) ->
        DefaultJDBCWrapper.executeStmt(conn, database, sql, MAX_EXEC_RESULTS.getInt(sessionState));
    executeUpdate = (conn, database, sql) ->
        DefaultJDBCWrapper.executeUpdate(conn, database, sql);
    executeUpdateWithPropagateException = DefaultJDBCWrapper::executeUpdate;
    sessionState.session.listenerManager().register(new LlapQueryExecutionListener());
    sessionState.session.sparkContext().addSparkListener(new HwcSparkListener());
    hwcSessionStateRef = new AtomicReference<>(HwcSessionState.OPEN);
    LOG.info("Created a new HWC session: {}", sessionId);
  }

  private enum HwcSessionState {
    OPEN, CLOSED;
  }

  private void ensureSessionOpen() {
    Preconditions.checkState(HwcSessionState.OPEN.equals(hwcSessionStateRef.get()), SESSION_CLOSED_MSG);
  }

  public Dataset<Row> q(String sql) {
    ensureSessionOpen();
    return executeQuery(sql);
  }

  public Dataset<Row> executeQuery(String sql) {
    return executeSmart(EXECUTE_QUERY_LLAP, sql, null);
  }

  public Dataset<Row> executeQuery(String sql, boolean useSplitsEqualToSparkCores) {
    return executeSmart(EXECUTE_QUERY_LLAP, sql, getCoresInSparkCluster());
  }

  public Dataset<Row> executeQuery(String sql, int numSplitsToDemand) {
    return executeSmart(EXECUTE_QUERY_LLAP, sql, numSplitsToDemand);
  }

  private Dataset<Row> executeQueryInternal(String sql, Integer numSplitsToDemand) {
    ensureSessionOpen();
    DataFrameReader dfr = session().read().format(HIVE_WAREHOUSE_CONNECTOR_INTERNAL).option("query", sql)
        .option(HWC_SESSION_ID_KEY, sessionId);
    if (numSplitsToDemand != null) {
      dfr.option(JobUtil.SESSION_QUERIES_FOR_GET_NUM_SPLITS, setSplitPropertiesQuery(numSplitsToDemand));
    }
    return dfr.load();
  }

  static void addResourceIdToSession(String sessionId, String resourceId) {
    LOG.info("Adding resource: {} to current session: {}", resourceId, sessionId);
    synchronized (RESOURCE_IDS_BY_SESSION_ID) {
      RESOURCE_IDS_BY_SESSION_ID.putIfAbsent(sessionId, new HashSet<>());
      RESOURCE_IDS_BY_SESSION_ID.get(sessionId).add(resourceId);
    }
  }

  static void closeAndRemoveResourceFromSession(String sessionId, String resourceId) throws IOException {
    Set<String> resourceIds;
    boolean resourcePresent;
    synchronized (RESOURCE_IDS_BY_SESSION_ID) {
      resourceIds = RESOURCE_IDS_BY_SESSION_ID.get(sessionId);
      resourcePresent = resourceIds != null && resourceIds.remove(resourceId);
    }
    if (resourcePresent) {
      LOG.info("Remove and close resource: {} from current session: {}", resourceId, sessionId);
      closeLlapResources(sessionId, resourceId);
    }
  }

  private static void closeLlapResources(String sessionId, String resourceId) throws IOException {
    LOG.info("Closing llap resource: {} for current session: {}", resourceId, sessionId);
    LlapBaseInputFormat.close(resourceId);
  }

  private static void closeSessionResources(String sessionId) throws IOException {
    LOG.info("Closing all resources for current session: {}", sessionId);
    Set<String> resourcesIds;
    synchronized (RESOURCE_IDS_BY_SESSION_ID) {
      resourcesIds = RESOURCE_IDS_BY_SESSION_ID.remove(sessionId);
    }
    if (resourcesIds != null && !resourcesIds.isEmpty()) {
      for (String resourceId : resourcesIds) {
        closeLlapResources(sessionId, resourceId);
      }
    }
  }

  private String setSplitPropertiesQuery(int numSplitsToDemand) {
    return String.format("SET tez.grouping.split-count=%s", numSplitsToDemand);
  }

  private int getCoresInSparkCluster() {
    //this was giving good results(number of cores) when tested in standalone and yarn mode(without dynamicAllocation)
    return session().sparkContext().defaultParallelism();
  }

  public Dataset<Row> execute(String sql) {
    return executeSmart(EXECUTE_HIVE_JDBC, sql, null);
  }

  private Dataset<Row> executeSmart(ExecutionMethod current, String sql, Integer numSplitsToDemand) {
    ExecutionMethod resolved = resolveExecutionMethod(
        BooleanUtils.toBoolean(HWConf.SMART_EXECUTION.getString(sessionState)), current, sql);
    if (EXECUTE_HIVE_JDBC.equals(resolved)) {
      return executeInternal(sql);
    }
    return executeQueryInternal(sql, numSplitsToDemand);
  }

  private Dataset<Row> executeInternal(String sql) {
    ensureSessionOpen();
    try (Connection conn = getConnector.get()) {
      DriverResultSet drs = executeStmt.apply(conn, DEFAULT_DB.getString(sessionState), sql);
      return drs.asDataFrame(session());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean executeUpdate(String sql) {
    return executeUpdate(sql, false);
  }

  @Override
  public boolean executeUpdate(String sql, boolean propagateException) {
    ensureSessionOpen();
    try (Connection conn = getConnector.get()) {
      return executeUpdateWithPropagateException.apply(conn, DEFAULT_DB.getString(sessionState), sql, propagateException);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean executeUpdateInternal(String sql, Connection conn) {
    ensureSessionOpen();
    return executeUpdateWithPropagateException.apply(conn, DEFAULT_DB.getString(sessionState), sql, true);
  }

  public Dataset<Row> executeInternal(String sql, Connection conn) {
    DriverResultSet drs = executeStmt.apply(conn, DEFAULT_DB.getString(sessionState), sql);
    return drs.asDataFrame(session());
  }

  public Dataset<Row> table(String sql) {
    ensureSessionOpen();
    DataFrameReader dfr = session().read().format(HIVE_WAREHOUSE_CONNECTOR_INTERNAL).option("table", sql)
        .option(HWC_SESSION_ID_KEY, sessionId);
    return dfr.load();
  }

  public SparkSession session() {
    ensureSessionOpen();
    return sessionState.session;
  }

  // Exposed for Python side.
  public HiveWarehouseSessionState sessionState() {
    return sessionState;
  }

  SparkConf conf() {
    return sessionState.session.sparkContext().getConf();
  }

  /* Catalog helpers */
  public void setDatabase(String name) {
    ensureSessionOpen();
    HWConf.DEFAULT_DB.setString(sessionState, name);
  }

  public Dataset<Row> showDatabases() {
    ensureSessionOpen();
    return execute(HiveQlUtil.showDatabases());
  }

  public Dataset<Row> showTables() {
    ensureSessionOpen();
    return execute(HiveQlUtil.showTables(DEFAULT_DB.getString(sessionState)));
  }

  public Dataset<Row> describeTable(String table) {
    ensureSessionOpen();
    return execute(HiveQlUtil.describeTable(DEFAULT_DB.getString(sessionState), table));
  }

  public void dropDatabase(String database, boolean ifExists, boolean cascade) {
    executeUpdate(HiveQlUtil.dropDatabase(database, ifExists, cascade));
  }

  public void dropTable(String table, boolean ifExists, boolean purge) {
    ensureSessionOpen();
    try (Connection conn = getConnector.get()) {
      executeUpdateInternal(HiveQlUtil.useDatabase(DEFAULT_DB.getString(sessionState)), conn);
      String dropTable = HiveQlUtil.dropTable(table, ifExists, purge);
      executeUpdateInternal(dropTable, conn);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void createDatabase(String database, boolean ifNotExists) {
    executeUpdate(HiveQlUtil.createDatabase(database, ifNotExists), true);
  }

  public CreateTableBuilder createTable(String tableName) {
    ensureSessionOpen();
    return new CreateTableBuilder(this, DEFAULT_DB.getString(sessionState), tableName);
  }

  @Override
  public MergeBuilder mergeBuilder() {
    ensureSessionOpen();
    return new MergeBuilderImpl(this, DEFAULT_DB.getString(sessionState));
  }

  @Override
  public void cleanUpStreamingMeta(String queryCheckpointDir, String dbName, String tableName) {
    ensureSessionOpen();
    try {
      new StreamingMetaCleaner(sessionState, queryCheckpointDir, dbName, tableName).clean();
    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    Preconditions.checkState(hwcSessionStateRef.compareAndSet(HwcSessionState.OPEN, HwcSessionState.CLOSED),
        SESSION_CLOSED_MSG);
    try {
      closeSessionResources(sessionId);
    } catch (IOException e) {
      throw new RuntimeException("Error while closing resources attached to session: " + sessionId);
    }
  }

}

