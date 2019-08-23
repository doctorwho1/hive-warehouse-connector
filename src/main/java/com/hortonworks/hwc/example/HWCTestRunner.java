package com.hortonworks.hwc.example;

import com.hortonworks.hwc.HiveWarehouseSession;
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl;
import org.apache.spark.sql.SparkSession;

/**
 * To test HWC java APIs with spark-submit.
 *
 * To run:
 * spark-submit --class com.hortonworks.hwc.example.HWCTestRunner --master local --deploy-mode client <path_to_hive-warehouse-connector-assembly-*.jar>
 *
 * This assumes that all the configurations required for HWC are set in spark-defaults.conf since spark-submit uses spark-defaults.conf by default.
 *
 */
public class HWCTestRunner {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName(HWCTestRunner.class.getName())
        .getOrCreate();

    // let it take properties from spark-defaults.conf
    HiveWarehouseSessionImpl session = HiveWarehouseSession.session(spark).build();
    System.out.println("========Showing databases========");
    session.showDatabases().show(false);
    System.out.println("========Showing databases========");

    // more test code here....

    spark.stop();
  }

}
