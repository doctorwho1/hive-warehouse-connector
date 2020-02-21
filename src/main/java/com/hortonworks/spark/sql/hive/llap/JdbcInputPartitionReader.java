package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.math.BigDecimal;
import java.util.Map;

public class JdbcInputPartitionReader implements InputPartitionReader<InternalRow> {
  private final String query;
  private final StructType schema;
  private Connection conn;
  private PreparedStatement stmt;
  private ResultSet resultSet;
  private final int numCols;
  private final Object[] rowData;

  private static Logger LOG = LoggerFactory.getLogger(JdbcInputPartitionReader.class);

  JdbcInputPartitionReader(String query, Map<String, String> options, StructType schema) throws Exception {
    this.query = query;
    this.schema = schema;

    this.conn = getConnection(options);
    this.stmt = conn.prepareStatement(query);
    int maxRows = Integer.parseInt(HWConf.MAX_EXEC_RESULTS.getFromOptionsMap(options));
    stmt.setMaxRows(maxRows);

    // Since the query might run in remote spark executor app, we need to set database before running
    // actual query.
    String dbName = HWConf.DEFAULT_DB.getFromOptionsMap(options);
    useDatabase(conn, dbName);
    this.resultSet = stmt.executeQuery();
    this.numCols = resultSet.getMetaData().getColumnCount();
    this.rowData = new Object[numCols];

    LOG.info("Execution via JDBC connection for query: " + query);
  }

  private Connection getConnection(Map<String, String> options) {
    String url = options.get(QueryExecutionUtil.HIVE_JDBC_URL_FOR_EXECUTOR);
    String user = HWConf.USER.getFromOptionsMap(options);
    String dbcp2Configs = HWConf.DBCP2_CONF.getFromOptionsMap(options);
    return QueryExecutionUtil.getConnection(url, user, dbcp2Configs);
  }

  private void useDatabase(Connection conn, String dbName) throws SQLException {
    if (dbName != null) {
      PreparedStatement stmt = conn.prepareStatement("USE " + dbName);
      stmt.execute();
      stmt.close();
    }
  }

  private Object normalizeColValue(Object colValue, DataType dataType) {
    if (colValue instanceof String) {
      colValue = UTF8String.fromString((String)colValue);
    } else if (colValue instanceof BigDecimal) {
      colValue = Decimal.apply((BigDecimal)colValue);
    } else if (colValue instanceof Double && dataType instanceof FloatType) {
      colValue = ((Double)colValue).floatValue();
    }
    return colValue;
  }

  private void closeResultSet() {
    try {
      if ((resultSet != null) && !resultSet.isClosed()) {
        resultSet.close();
      }
      resultSet = null;
    } catch (SQLException e) {
      LOG.warn("Failed to close ResultSet for query: " + query);
    }
  }

  private void closeStmt() {
    try {
      if ((stmt != null) && !stmt.isClosed()) {
        stmt.close();
      }
      stmt = null;
    } catch (SQLException e) {
      LOG.warn("Failed to close PreparedStatement for query: " + query);
    }
  }

  private void closeConnection() {
    try {
      if ((conn != null) && !conn.isClosed()) {
        conn.close();
      }
      conn = null;
    } catch (SQLException e) {
      LOG.warn("Failed to close JDBC Connection for query: " + query);
    }
  }

  @Override
  public boolean next() throws IOException {
    try {
      boolean hasNext = resultSet.next();
      if (hasNext) {
        for (int i = 0; i < numCols; i++) {
          DataType dataType = null;
          if (schema.size() > i) {
            dataType = schema.apply(i).dataType();
          }
          rowData[i] = normalizeColValue(resultSet.getObject(i + 1), dataType);
        }
      }
      return hasNext;
    } catch (SQLException e) {
      LOG.error("Failed to traverse the ResultSet for the query: " + query);
      throw new IOException(e);
    }
  }

  @Override
  public InternalRow get() {
    return new GenericInternalRow(rowData);
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing resources for the query: " + query);
    closeResultSet();
    closeStmt();
    closeConnection();
  }
}

