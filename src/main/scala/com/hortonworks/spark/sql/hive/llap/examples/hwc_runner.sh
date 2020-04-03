echo " 🚀🚀🚀🚀🚀 🚀🚀🚀🚀🚀 Running HWCRunner 🚀🚀🚀🚀🚀 🚀🚀🚀🚀🚀"

$SPARK_HOME/bin/spark-submit \
--class com.hortonworks.spark.sql.hive.llap.examples.HWCRunner \
--master local --deploy-mode client \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://localhost:10000?org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;hive.support.concurrency=true;hive.enforce.bucketing=true;hive.exec.dynamic.partition.mode=nonstrict;" \
--conf spark.datasource.hive.warehouse.load.staging.dir=hdfs://localhost:54587/tmp \
--conf spark.hadoop.hive.zookeeper.quorum=localhost:53231 \
--conf spark.hadoop.hive.llap.daemon.service.hosts=@llap_MiniLlapCluster \
/Users/schaurasia/Documents/repos/hortonworks/hive-warehouse-connector/target/scala-2.11/hive-warehouse-connector-assembly-1.0.0-SNAPSHOT.jar
