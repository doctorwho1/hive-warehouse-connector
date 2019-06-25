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

import java.util.concurrent.Callable;

import com.hortonworks.hwc.MergeBuilder;
import org.junit.Before;
import org.junit.Test;

import static com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilderTest.*;
import static com.hortonworks.spark.sql.hive.llap.TestSecureHS2Url.TEST_HS2_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class HiveWarehouseSessionHiveQlTest extends SessionTestBase {

    private HiveWarehouseSession hive;
    private int mockExecuteResultSize;


    @Before
    public void setUp() {
        super.setUp();
        HiveWarehouseSessionState sessionState =
                HiveWarehouseBuilder
                        .session(session)
                        .userPassword(TEST_USER, TEST_PASSWORD)
                        .hs2url(TEST_HS2_URL)
                        .dbcp2Conf(TEST_DBCP2_CONF)
                        .maxExecResults(TEST_EXEC_RESULTS_MAX)
                        .defaultDB(TEST_DEFAULT_DB)
                        .sessionStateForTest();
         hive = new MockHiveWarehouseSessionImpl(sessionState);
         mockExecuteResultSize =
                 MockHiveWarehouseSessionImpl.testFixture().data.size();
    }


    @Test
    void testExecuteQuery() {
        assertEquals(hive.executeQuery("SELECT * FROM t1").count(),
                SimpleMockConnector.SimpleMockDataReader.RESULT_SIZE);
    }

    @Test
    void testUnqualifiedTable() {
        assertEquals(hive.table("t1").count(),
            SimpleMockConnector.SimpleMockDataReader.RESULT_SIZE);
    }

    @Test
    void testQualifiedTable() {
        assertEquals(hive.table("default.t1").count(),
            SimpleMockConnector.SimpleMockDataReader.RESULT_SIZE);
    }

    @Test
    void testSetDatabase() {
        hive.setDatabase(TEST_DEFAULT_DB);
    }

    @Test
    void testDescribeTable() {
        assertEquals(hive.describeTable("testTable").count(),
               mockExecuteResultSize);
    }

    @Test
    void testCreateDatabase() {
        hive.createDatabase(TEST_DEFAULT_DB, false);
        hive.createDatabase(TEST_DEFAULT_DB, true);
    }

    @Test
    void testDropDatabase() {
        //Tests if generated syntax is parsed by HiveParser
        hive.dropDatabase(TEST_DEFAULT_DB, false, false);
        hive.dropDatabase(TEST_DEFAULT_DB, false, true);
        hive.dropDatabase(TEST_DEFAULT_DB, true, false);
        hive.dropDatabase(TEST_DEFAULT_DB, true, true);
    }

    @Test
    void testShowTable() {
        assertEquals(hive.showTables().count(), mockExecuteResultSize);
    }

    @Test
    void testNoOperationPermittedAfterSessionClose() {
        hive.close();
        expectIllegalStateException(() -> hive.showDatabases());
        expectIllegalStateException(() -> hive.showTables());
        expectIllegalStateException(() -> hive.executeUpdate("insert overwrite table a select * from b"));
        expectIllegalStateException(() -> hive.executeUpdate("insert overwrite table a select * from b", true));
        expectIllegalStateException(() -> hive.execute("select * from sky"));
        expectIllegalStateException(() -> hive.executeQuery("select * from sky"));
        expectIllegalStateException(() -> hive.describeTable("table1"));
        expectIllegalStateException(() -> {
            hive.setDatabase("blah");
            return null;
        });

        expectIllegalStateException(() -> {
            hive.createDatabase("blah", true);
            return null;
        });

        expectIllegalStateException(() -> {
            hive.dropDatabase("blah", true, true);
            return null;
        });
        expectIllegalStateException(() -> {
            hive.dropTable("blah", true, true);
            return null;
        });
        expectIllegalStateException(() -> {
             hive.createTable("a")
                .column("a", "string").create();
             return null;
        });
        expectIllegalStateException(() -> {
            hive.mergeBuilder().mergeInto("target_table", "t")
                .using("source_table", "s")
                .on("t.id = s.id")
                .whenMatchedThenUpdate("t.col1 = s.col1", "col2=s.col2", "col3=s.col3")
                .whenMatchedThenDelete("t.col4 = s.col4")
                .whenNotMatchedInsert("t.col1 != s.col1", "s.col1", "s.col2", "s.col3", "s.col4")
                .merge();
            return null;
        });

        // try closing again
        expectIllegalStateException(() -> {
            hive.close();
            return null;
        });
    }

    private void  expectIllegalStateException(Callable action) {
        Exception exception = null;
        try {
            action.call();
        } catch (Exception e) {
            exception = e;
        }
        assertTrue(exception instanceof IllegalStateException);
    }

    @Test
    void testCreateTable() {
        com.hortonworks.hwc.CreateTableBuilder builder =
          hive.createTable("TestTable");
        builder
          .ifNotExists()
          .column("id", "int")
          .column("val", "string")
          .partition("id", "int")
          .clusterBy(100, "val")
          .prop("key", "value")
          .create();
    }

    //we cannot do a negative/positive test for propagateException as for UTs we only parse query and return true
    @Test
    void testCreateTableWithPropagateException() {
        com.hortonworks.hwc.CreateTableBuilder builder =
            hive.createTable("TestTable");
        builder
            .ifNotExists()
            .column("id", "int")
            .column("val", "string")
            .partition("id", "int")
            .clusterBy(100, "val")
            .prop("key", "value")
            .propagateException()
            .create();
    }


    @Test
    void testMergeBuilder() {
        MergeBuilder mergeBuilder = hive.mergeBuilder();
        mergeBuilder.mergeInto("target_table", "t")
            .using("source_table", "s")
            .on("t.id = s.id")
            .whenMatchedThenUpdate("t.col1 = s.col1", "col2=s.col2", "col3=s.col3")
            .whenMatchedThenDelete("t.col4 = s.col4")
            .whenNotMatchedInsert("t.col1 != s.col1", "s.col1", "s.col2", "s.col3", "s.col4")
            .merge();
    }

    @Test
    void testMergeBuilderWithSourceExpression() {
        MergeBuilder mergeBuilder = hive.mergeBuilder();
        mergeBuilder.mergeInto("target_table", "t")
            .using("(select * from source_table)", "s")
            .on("t.id = s.id")
            .whenMatchedThenUpdate("t.col1 = s.col1", "col2=s.col2", "col3=s.col3")
            .whenMatchedThenDelete("t.col4 = s.col4")
            .whenNotMatchedInsert("t.col1 != s.col1", "s.col1", "s.col2", "s.col3", "s.col4")
            .merge();
    }

    @Test
    void testMergeBuilderWithoutMatchExpressions() {
        MergeBuilder mergeBuilder = hive.mergeBuilder();
        mergeBuilder.mergeInto("target_table", "t")
            .using("source_table", "s")
            .on("t.id = s.id")
            .whenMatchedThenUpdate(null, "col2=s.col2", "col3=s.col3")
            .whenMatchedThenDelete(null)
            .whenNotMatchedInsert(null, "s.col1", "s.col2", "s.col3", "s.col4")
            .merge();
    }
}
