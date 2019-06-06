package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.ExecutionMethod;
import org.apache.spark.sql.Row;
import org.junit.Test;

import static com.hortonworks.spark.sql.hive.llap.TestSecureHS2Url.TEST_HS2_URL;
import static com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.ExecutionMethod.*;
import static com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.resolveExecutionMethod;
import static org.junit.Assert.assertEquals;

public class TestReadSupport extends SessionTestBase {

  @Test
  public void testReadSupport() {
    HiveWarehouseSession hive = HiveWarehouseBuilder.
        session(session).
        hs2url(TEST_HS2_URL).
        build();
    HiveWarehouseSessionImpl impl = (HiveWarehouseSessionImpl) hive;
    impl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL = "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector";
    Row[] rows = (Row[]) hive.executeQuery("SELECT a from fake").sort("a").collect();
    for(int i = 0; i < MockHiveWarehouseConnector.testVector.length; i++) {
      assertEquals(rows[i].getInt(0), MockHiveWarehouseConnector.testVector[i]);
    }
  }

  @Test
  public void testCountSupport() {
    HiveWarehouseSession hive = HiveWarehouseBuilder.
        session(session).
        hs2url(TEST_HS2_URL).
        build();
    HiveWarehouseSessionImpl impl = (HiveWarehouseSessionImpl) hive;
    impl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL = "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector";
    long count = hive.executeQuery("SELECT a from fake").count();
    assertEquals(count, MockHiveWarehouseConnector.COUNT_STAR_TEST_VALUE);
  }

  @Test
  public void testSmartExecutionCases() {

    testResolveExecutionMethodForSmartExecution("show tables", EXECUTE_HIVE_JDBC);
    testResolveExecutionMethodForSmartExecution("show databases", EXECUTE_HIVE_JDBC);
    testResolveExecutionMethodForSmartExecution("describe me", EXECUTE_HIVE_JDBC);

    testResolveExecutionMethodForSmartExecution("select * from sky", EXECUTE_QUERY_LLAP);
    testResolveExecutionMethodForSmartExecution("explain select * from sky", EXECUTE_HIVE_JDBC);
    testResolveExecutionMethodForSmartExecution("create table another_sky as select * from sky", EXECUTE_HIVE_JDBC);

    testResolveExecutionMethodForSmartExecution("select t1.a from (select * from sky) t1", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("with q1 as (select * from sky limit 10) select * from q1", EXECUTE_QUERY_LLAP);
    testResolveExecutionMethodForSmartExecution(
        "with q1 as (select * from movie limit 10), q2 as (select * from sky limit 10) select * from q1 union all select * from q2", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("select *, floor(1.19) from sky", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("SELECT a.* FROM a JOIN b ON (a.id = b.id AND a.department = b.department)", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("SELECT a.val, b.val, c.val FROM a JOIN b ON (a.key = b.key1) JOIN c ON (c.key = b.key2)", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("SELECT a.val, b.val, c.val FROM a JOIN b ON (a.key = b.key1) JOIN c ON (c.key = b.key2)", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("SELECT a, SUM(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM T", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("SELECT a, COUNT(b) OVER (PARTITION BY c) AS b_count, SUM(b) OVER (PARTITION BY c) b_sum FROM T", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("SELECT a, SUM(b) OVER w FROM T WINDOW w AS (PARTITION BY c ORDER BY d ROWS UNBOUNDED PRECEDING)", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("SELECT a, LAG(a, 3, 0), LEAD(a) OVER (PARTITION BY b ORDER BY C) FROM T", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("SELECT * FROM source TABLESAMPLE(BUCKET 3 OUT OF 32 ON rand()) s", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("SELECT * FROM source TABLESAMPLE(0.1 PERCENT) s", EXECUTE_QUERY_LLAP);

    testResolveExecutionMethodForSmartExecution("select count(distinct cs_order_number) as `order count` from catalog_sales order by count(distinct cs_order_number) limit 100", EXECUTE_QUERY_LLAP);

  }

  private void testResolveExecutionMethodForSmartExecution(final String sql, final ExecutionMethod expectedWhenEnabled) {

    ExecutionMethod resolved;
    resolved = resolveExecutionMethod(true, EXECUTE_HIVE_JDBC, sql);
    assertEquals(expectedWhenEnabled, resolved);

    resolved = resolveExecutionMethod(true, EXECUTE_QUERY_LLAP, sql);
    assertEquals(expectedWhenEnabled, resolved);

  }

}
