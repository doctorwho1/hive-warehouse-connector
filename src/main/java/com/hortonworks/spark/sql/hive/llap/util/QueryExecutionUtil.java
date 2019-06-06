package com.hortonworks.spark.sql.hive.llap.util;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import static com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.ExecutionMethod.EXECUTE_HIVE_JDBC;
import static com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.ExecutionMethod.EXECUTE_QUERY_LLAP;

public final class QueryExecutionUtil {

  public enum ExecutionMethod {
    EXECUTE_HIVE_JDBC, EXECUTE_QUERY_LLAP
  }

  /**
   *
   * When smartExecutionEnabled is set, this method tries to figure out best execution method(hive jdbc or hive llap + spark data readers).
   * Currently it examines the token at root node of parse tree to separate out various patterns of select queries from others.
   * All select queries are executed via EXECUTE_QUERY_LLAP as of now.
   */
  public static ExecutionMethod resolveExecutionMethod(boolean smartExecutionEnabled,
                                                       ExecutionMethod currentExecutionMethod, String sql) {
    ExecutionMethod resolved = currentExecutionMethod;
    if (smartExecutionEnabled) {
      ASTNode tree;
      try {
        HiveConf conf = new HiveConf();
        conf.set("_hive.hdfs.session.path", "/tmp/some_dummy_path");
        conf.set("_hive.local.session.path", "/tmp/some_dummy_path");
        Context ctx = new Context(conf);
        tree = ParseUtils.parse(sql, ctx);
      } catch (ParseException | IOException e) {
        throw new RuntimeException(e);
      }
      //select queries have token of type HiveParser.TOK_QUERY at top level in their parse tree.
      if (HiveParser.TOK_QUERY == tree.getType()) {
        resolved = EXECUTE_QUERY_LLAP;
      } else {
        //Apart from select queries, all other queries can be executed via execute()
        resolved = EXECUTE_HIVE_JDBC;
      }
    }
    return resolved;
  }

}
