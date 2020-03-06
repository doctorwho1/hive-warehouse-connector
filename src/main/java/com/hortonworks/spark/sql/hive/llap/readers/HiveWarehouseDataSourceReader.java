package com.hortonworks.spark.sql.hive.llap.readers;

import com.google.common.base.Preconditions;
import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper;
import com.hortonworks.spark.sql.hive.llap.common.DriverResultSet;
import com.hortonworks.spark.sql.hive.llap.common.HWConf;
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl;
import com.hortonworks.spark.sql.hive.llap.common.SerializableLlapInputSplit;
import com.hortonworks.spark.sql.hive.llap.common.StatementType;
import com.hortonworks.spark.sql.hive.llap.common.CommonBroadcastInfo;
import com.hortonworks.spark.sql.hive.llap.common.HwcResource;
import com.hortonworks.spark.sql.hive.llap.readers.batch.HiveWarehouseBatchDataReaderFactory;
import com.hortonworks.spark.sql.hive.llap.readers.row.HiveWarehouseDataReaderFactory;
import com.hortonworks.spark.sql.hive.llap.readers.batch.HiveCountBatchDataReaderFactory;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.hortonworks.spark.sql.hive.llap.FilterPushdown.buildWhereClause;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.projections;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.randomAlias;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.selectProjectAliasFilter;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.selectStar;
import static com.hortonworks.spark.sql.hive.llap.util.JobUtil.replaceSparkHiveDriver;
import static scala.collection.JavaConversions.asScalaBuffer;

/**
 * 1. Spark pulls the unpruned schema -> readSchema()
 * 2. Spark pulls factories, where factory/task are 1:1
 *       -> if (enableBatchRead)
 *            createBatchDataReaderFactories(..)
 *          else
 *            createDataReaderFactories(..)
 */
public class HiveWarehouseDataSourceReader implements SupportsScanColumnarBatch {

  //The original schema
  protected StructType baseSchema = null;

  //The pruned schema
  protected StructType schema = null;

  //SessionConfigSupport options
  protected Map<String, String> options;

  protected JobConf jobConf;
  protected CommonBroadcastInfo commonBroadcastInfo;

  // Enable ColumnarBatch reader by default.
  private boolean enableBatchRead;
  private HwcResource hwcResource;
  private final String sessionId;

  private static Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataSourceReader.class);

  public HiveWarehouseDataSourceReader(Map<String, String> options) {
    this.options = options;
    sessionId = getCurrentSessionId();
    enableBatchRead = shouldEnableBatchRead();
  }

  private boolean shouldEnableBatchRead() {
    // If the number of projected columns exceed the supported limit for batch reader, then use non-batch reader
    int columnsLimitForBatchDataReader
            = Integer.parseInt(HWConf.BATCH_DATAREADER_COLUMNS_LIMIT.getFromOptionsMap(this.options));
    Preconditions.checkState(columnsLimitForBatchDataReader > 0,
            HWConf.INVALID_BATCH_DATAREADER_COLUMNS_LIMIT_CONFIG_ERR_MSG);

    // Check if we need to enable batch read. Need to check based on input config.
    return (this.readSchema().length() <= columnsLimitForBatchDataReader);
  }

  //if(schema is empty) -> df.count()
  //else if(using table option) -> select *
  //else -> SELECT <COLUMNS> FROM (<RAW_SQL>) WHERE <FILTER_CLAUSE>
  private String getQueryString(String[] requiredColumns, Filter[] filters) throws Exception {
    String selectCols = "count(*)";
    if (requiredColumns.length > 0) {
      selectCols = projections(requiredColumns);
    }
    String baseQuery;
    if (getQueryType() == StatementType.FULL_TABLE_SCAN) {
      baseQuery = selectStar(options.get("table"));
    } else {
      baseQuery = options.get("query");
    }

    String query = selectProjectAliasFilter(selectCols, baseQuery, randomAlias(), buildWhereClauseFromFilters(filters));
    LOG.info("Final Query: {}", query);
    return query;
  }

  private StatementType getQueryType() {
    return StatementType.fromOptions(options);
  }

  private String getCurrentSessionId() {
    String sessionId = options.get(HiveWarehouseSessionImpl.HWC_SESSION_ID_KEY);
    Preconditions.checkNotNull(sessionId,
        "session id cannot be null, forgot to initialize HiveWarehouseSession???");
    return sessionId;
  }

  protected StructType getTableSchema() throws Exception {
    replaceSparkHiveDriver();

    StatementType queryKey = getQueryType();
    String query;
    if (queryKey == StatementType.FULL_TABLE_SCAN) {
      String dbName = HWConf.DEFAULT_DB.getFromOptionsMap(options);
      SchemaUtil.TableRef tableRef = SchemaUtil.getDbTableNames(dbName, options.get("table"));
      query = selectStar(tableRef.databaseName, tableRef.tableName);
    } else {
      query = options.get("query");
    }
    LlapBaseInputFormat llapInputFormat = null;
    try {
      JobConf conf = JobUtil.createJobConf(options, query);
      llapInputFormat = new LlapBaseInputFormat(false, Long.MAX_VALUE);
      InputSplit[] splits = llapInputFormat.getSplits(conf, 0);
      LlapInputSplit schemaSplit = (LlapInputSplit) splits[0];
      Schema schema = schemaSplit.getSchema();
      return SchemaUtil.convertSchema(schema);
    } finally {
      if(llapInputFormat != null) {
        close();
      }
    }
  }

  protected String buildWhereClauseFromFilters(Filter[] filters) {
    Seq<Filter> filterSeq = asScalaBuffer(Arrays.asList(filters)).seq();
    return buildWhereClause(baseSchema, filterSeq);
  }

  @Override
  public StructType readSchema() {
    try {
      if (schema == null) {
        this.schema = getTableSchema();
        this.baseSchema = this.schema;
      }
      return schema;
    } catch (Exception e) {
      LOG.error("Unable to read table schema");
      throw new RuntimeException(e);
    }
  }

  public Filter[] getPushedFilters() {
    return new Filter[0];
  }

  public void close() {
    try {
      HiveWarehouseSessionImpl.closeAndRemoveResourceFromSession(sessionId, hwcResource);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public boolean enableBatchRead() {
    return enableBatchRead;
  }

  /* Method used by Spark executor if enableBatchRead = false */
  @Override
  public List<DataReaderFactory<Row>> createDataReaderFactories() {
    LOG.info("Creating non-batch data reader factories.");
    try {
      assert(this.schema.length() > 0);
      String queryString = getQueryString(SchemaUtil.columnNames(schema), getPushedFilters());
      return getDataReaderSplitsFactories(queryString);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /* Method used by Spark executor enableBatchRead = true */
  @Override
  public List<DataReaderFactory<ColumnarBatch>> createBatchDataReaderFactories() {
    LOG.info("Creating batch data reader factories.");
    try {
      String queryString = getQueryString(SchemaUtil.columnNames(schema), getPushedFilters());

      // if count(*) query, then return corresponding reader factories.
      boolean countStarQuery = (this.schema.length() == 0);
      if (countStarQuery) {
        LOG.info("Executing count with query: {}", queryString);
        return getBatchDataReaderCountStarFactories(queryString);
      } else {
        return getDataReaderSplitsFactories(queryString);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected <T> List<DataReaderFactory<T>> getDataReaderSplitsFactories(String query) throws IOException {
    List<DataReaderFactory<T>> tasks = new ArrayList<>();
    InputSplit[] splits = getSplits(query);
    if (splits.length > 2) {
      LOG.info("Serializing {} actual splits to send to executors", (splits.length - 2));
      byte[] serializedJobConf = JobUtil.serializeJobConf(jobConf);
      long arrowAllocatorMax = getArrowAllocatorMax();
      long start = System.currentTimeMillis();
      for (int i = 2; i < splits.length; i++) {
        tasks.add(getDataReaderFactory(splits[i], serializedJobConf, arrowAllocatorMax, commonBroadcastInfo));
      }
      long end = System.currentTimeMillis();
      LOG.info("Serialized {} actual splits in {} millis", (splits.length - 2), (end - start));
    }
    return tasks;
  }

  protected <T> DataReaderFactory<T> getDataReaderFactory(InputSplit split, byte[] serializedJobConf,
                                                          long arrowAllocatorMax,
                                                          CommonBroadcastInfo commonBroadcastInfo) {
    if (enableBatchRead) {
      return (DataReaderFactory<T>)(new HiveWarehouseBatchDataReaderFactory(split, serializedJobConf,
                                                              arrowAllocatorMax, commonBroadcastInfo));
    } else {
      return (DataReaderFactory<T>)(new HiveWarehouseDataReaderFactory(split, serializedJobConf,
                                                              arrowAllocatorMax, commonBroadcastInfo));
    }
  }

  protected List<DataReaderFactory<ColumnarBatch>> getBatchDataReaderCountStarFactories(String query) {
    List<DataReaderFactory<ColumnarBatch>> tasks = new ArrayList<>(100);
    long count = getCount(query);
    String numTasksString = HWConf.COUNT_TASKS.getFromOptionsMap(options);
    int numTasks = Integer.parseInt(numTasksString);
    long numPerTask = count / (numTasks - 1);
    long numLastTask = count % (numTasks - 1);
    for (int i = 0; i < (numTasks - 1); i++) {
      tasks.add(new HiveCountBatchDataReaderFactory(numPerTask));
    }
    tasks.add(new HiveCountBatchDataReaderFactory(numLastTask));
    return tasks;
  }

  private InputSplit[] getSplits(String query) {
    try {
      jobConf = JobUtil.createJobConf(options, query);
      LlapBaseInputFormat llapInputFormat = new LlapBaseInputFormat(false, Long.MAX_VALUE);
      LOG.info("Additional props for generating splits: {}", options.get(JobUtil.SESSION_QUERIES_FOR_GET_NUM_SPLITS));
      //numSplits arg not currently supported, use 1 as dummy arg
      InputSplit[]  splits = llapInputFormat.getSplits(jobConf, 1);
      LOG.info("Number of splits generated: {}", splits.length);

      if (splits.length > 2) {
        commonBroadcastInfo = prepareCommonBroadcastInfo(splits);
      } else {
        LOG.warn("No actual splits generated for query: {}", query);
      }
      return splits;
    } catch (IOException e) {
      LOG.error("Unable to submit query to HS2");
      throw new RuntimeException(e);
    } finally {
      // add handle id for HiveWarehouseSessionImpl#close()
      hwcResource = new HwcResource(options.get(JobUtil.LLAP_HANDLE_ID), commonBroadcastInfo);
      HiveWarehouseSessionImpl.addResourceIdToSession(sessionId, hwcResource);
    }
  }

  private CommonBroadcastInfo prepareCommonBroadcastInfo(InputSplit[] splits) {
    SparkContext sparkContext = SparkSession.getActiveSession().get().sparkContext();

    // populate actual splits with schema and planBytes[]
    LlapInputSplit schemaSplit = (LlapInputSplit) splits[0];
    LlapInputSplit planSplit = (LlapInputSplit) splits[1];

    // Don't broadcast if splits have already been broadcast for some previous execution
    if (commonBroadcastInfo != null
        && commonBroadcastInfo.getSchemaSplit().isValid()
        && commonBroadcastInfo.getPlanSplit().isValid()
        && planSplit.equals(commonBroadcastInfo.getPlanSplit().getValue().getLlapInputSplit())
        && schemaSplit.equals(commonBroadcastInfo.getSchemaSplit().getValue().getLlapInputSplit())) {
      return commonBroadcastInfo;
    }

    Broadcast<SerializableLlapInputSplit> planSplitBroadcast =
        broadcastSplit(sparkContext, planSplit);

    Broadcast<SerializableLlapInputSplit> schemaSplitBroadcast =
        broadcastSplit(sparkContext, schemaSplit);

    CommonBroadcastInfo commonBroadcastInfo = new CommonBroadcastInfo();
    commonBroadcastInfo.setSchemaSplit(schemaSplitBroadcast);
    commonBroadcastInfo.setPlanSplit(planSplitBroadcast);

    return commonBroadcastInfo;
  }

  private Broadcast<SerializableLlapInputSplit> broadcastSplit(SparkContext sparkContext, LlapInputSplit planSplit) {
    return sparkContext.broadcast(new SerializableLlapInputSplit(planSplit),
        SchemaUtil.classTag(SerializableLlapInputSplit.class));
  }

  private long getArrowAllocatorMax () {
    String arrowAllocatorMaxString = HWConf.ARROW_ALLOCATOR_MAX.getFromOptionsMap(options);
    long arrowAllocatorMax = (Long) HWConf.ARROW_ALLOCATOR_MAX.getDefaultValue();
    if (arrowAllocatorMaxString != null) {
      arrowAllocatorMax = Long.parseLong(arrowAllocatorMaxString);
    }
    LOG.debug("Ceiling for Arrow direct buffers {}", arrowAllocatorMax);
    return arrowAllocatorMax;
  }

  protected long getCount(String query) {
    try (Connection conn = getConnection()) {
      DriverResultSet rs = DefaultJDBCWrapper.executeStmt(conn, HWConf.DEFAULT_DB.getFromOptionsMap(options), query,
          Long.parseLong(HWConf.MAX_EXEC_RESULTS.getFromOptionsMap(options)));
      return rs.getData().get(0).getLong(0);
    } catch (SQLException e) {
      LOG.error("Failed to connect to HS2", e);
      throw new RuntimeException(e);
    }
  }

  private Connection getConnection() {
    String url = HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options);
    String user = HWConf.USER.getFromOptionsMap(options);
    String dbcp2Configs = HWConf.DBCP2_CONF.getFromOptionsMap(options);
    return DefaultJDBCWrapper.getConnector(Option.empty(), url, user, dbcp2Configs);
  }
}
