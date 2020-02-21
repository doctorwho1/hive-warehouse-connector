package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
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
 * 2. Spark pushes the pruned schema -> pruneColumns(..)
 * 3. Spark pushes the top-level filters -> pushFilters(..)
 * 4. Spark pulls the filters that are supported by datasource -> pushedFilters(..)
 * 5. Spark pulls factories, where factory/task are 1:1 -> planInputPartitions(..)
 */
public class JdbcDataSourceReader
    implements DataSourceReader, SupportsPushDownRequiredColumns, SupportsPushDownFilters {

  //The pruned schema
  private StructType schema = null;

  //The original schema
  private StructType baseSchema = null;

  //Pushed down filters
  //
  //"It's possible that there is no filters in the query and pushFilters(Filter[])
  // is never called, empty array should be returned for this case."
  private Filter[] pushedFilters = new Filter[0];

  //SessionConfigSupport options
  private final Map<String, String> options;

  private final static Logger LOG = LoggerFactory.getLogger(JdbcDataSourceReader.class);

  JdbcDataSourceReader(Map<String, String> options) {
    this.options = options;

    //this is a hack to prevent the following situation:
    //Spark(v 2.4.0) creates one instance of DataSourceReader to call readSchema() and then a new instance of DataSourceReader
    //to call pushFilters(), planBatchInputPartitions() etc. Since it uses different DataSourceReader instances,
    //and reads schema in former instance, schema remains null in the latter instance(which causes problems for other methods).
    //More discussion: http://apache-spark-user-list.1001560.n3.nabble.com/DataSourceV2-APIs-creating-multiple-instances-of-DataSourceReader-and-hence-not-preserving-the-state-tc33646.html
    //Also a null check on schema is already there in readSchema() to prevent initialization more than once just in case.
    readSchema();
  }

  //if(schema is empty) -> df.count()
  //else if(using table option) -> select *
  //else -> SELECT <COLUMNS> FROM (<RAW_SQL>) WHERE <FILTER_CLAUSE>
  private String getQueryString(String[] requiredColumns, Filter[] filters) {
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

    Seq<Filter> filterSeq = asScalaBuffer(Arrays.asList(filters)).seq();
    String whereClause = buildWhereClause(baseSchema, filterSeq);
    return selectProjectAliasFilter(selectCols, baseQuery, randomAlias(), whereClause);
  }

  private StatementType getQueryType() {
    return StatementType.fromOptions(options);
  }

  private StructType getTableSchema() throws Exception {
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

    // We need to obtain the result schema for the given query.
    try (Connection conn = QueryExecutionUtil.getConnection(options)) {
      return DefaultJDBCWrapper.resolveQuery(conn, HWConf.DEFAULT_DB.getFromOptionsMap(options), query);
    } catch (SQLException e) {
      LOG.error("Failed to connect to HS2", e);
      throw new RuntimeException(e);
    }
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

  //"returns unsupported filters."
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
  public List<InputPartition<InternalRow>> planInputPartitions() {
    try {
      String queryString = getQueryString(SchemaUtil.columnNames(schema), pushedFilters);
      return Collections.singletonList(new JdbcInputPartition(queryString, options, schema));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
