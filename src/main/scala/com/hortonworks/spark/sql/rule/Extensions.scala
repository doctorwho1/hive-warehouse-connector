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

package com.hortonworks.spark.sql.rule

import java.util.{HashMap => JHashMap, Map => JMap}
import java.util.regex.Pattern

import scala.collection.JavaConverters._

import org.apache.commons.lang.BooleanUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf

import com.hortonworks.spark.sql.hive.llap.{HiveWarehouseConnector, HiveWarehouseSessionImpl, HWConf}
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl.HWC_QUERY_MODE
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl.HWC_SESSION_ID_KEY
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.ExecutionMethod.EXECUTE_HIVE_JDBC
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil.ExecutionMethod.EXECUTE_QUERY_LLAP

class Extensions extends Function1[SparkSessionExtensions, Unit] {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(_ => new HWCSwitchRule)
  }
}

class HWCSwitchRule extends Rule[LogicalPlan] with Logging {
  val ACCESSTYPE_NONE: Byte = 1
  val ACCESSTYPE_READWRITE: Byte = 8

  private def getAccessType(tableMeta: CatalogTable): Int = {
    try {
      val method1 = tableMeta.getClass.getMethod("accessInfo")
      val accessInfo = method1.invoke(tableMeta)
      val method2 = accessInfo.getClass.getMethod("accessType")
      method2.invoke(accessInfo).asInstanceOf[Int]
    } catch {
      case n: NoSuchMethodException =>
        logInfo("Unknown access type; Assuming READWRITE")
        ACCESSTYPE_READWRITE
    }
  }

  private def getSessionOpts(keyPrefix: String, spark: SparkSession): Map[String, String] = {
    val conf = SQLConf.get

    val pattern = Pattern.compile(s"^spark\\.datasource\\.$keyPrefix\\.(.+)")
    conf.getAllConfs.flatMap { case (key, value) =>
      val m = pattern.matcher(key)
      if (m.matches() && m.groupCount() > 0) {
        Seq((m.group(1), value))
      } else {
        Seq.empty
      }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case r @ HiveTableRelation(tableMeta, _, _) if (getAccessType(tableMeta) == ACCESSTYPE_NONE) =>
      val dbName = tableMeta.identifier.database.getOrElse("default")
      val tableName = tableMeta.identifier.table

      val spark = SparkSession.getActiveSession.get
      val ds = new HiveWarehouseConnector

      val keyPrefix = ds.keyPrefix()

      var sessionOpts = getSessionOpts(keyPrefix, spark)

      // determine if we're configured to use LLAP
      val usingLlap = BooleanUtils.
        toBoolean(HWConf.READ_VIA_LLAP.getFromOptionsMap(sessionOpts.asJava))

      // If we're using JDBC, check that the mode is 'cluster'
      if (!usingLlap) {
        val jdbcMode = HWConf.READ_JDBC_MODE.getFromOptionsMap(sessionOpts.asJava)
        if (jdbcMode != "cluster") {
          // Don't switch, just keep the old Hive relation
          log.warn(s"JDBC mode is $jdbcMode; Not switching to HWC connector for " +
            s"table `${dbName}`.`${tableName}`")
          return r
        }
      }

      log.info(s"Switching to Hive Warehouse Connector for table `${dbName}`.`${tableName}`")

      // get the HiveWarehouseConnector session for this spark session
      val hwcSession = HWCSwitchRule.hwcSessions.synchronized {
        val prevSession = HWCSwitchRule.hwcSessions.get(spark)
        if (prevSession != null) {
          prevSession
        } else {
          val newSession = com.hortonworks.hwc.HiveWarehouseSession.session(spark).build()
          HWCSwitchRule.hwcSessions.put(spark, newSession)
          newSession
        }
      }

      val sessionId = hwcSession.getSessionId
      log.info(s"Hive Warehouse Connector session ID is $sessionId")

      // now that we've  created the Hive Warehouse Session, get opts again (since it's filled
      // in the resolved JDBC url for us)
      sessionOpts = getSessionOpts(keyPrefix, spark)

      // The scheme for specifying the table to the HWC differs depending on whether the
      // configuration specified LLAP or JDBC. This is due to CDPD-6892
      val tableOpts = if (usingLlap) {
        log.info("Specifying the table the LLAP way")
        Map("table" -> s"`${dbName}`.`${tableName}`",
          HWC_QUERY_MODE -> EXECUTE_QUERY_LLAP.name())
      } else {
        log.info("Specifying the table the JDBC way")
        val jdbcUrlForExecutor = hwcSession.getJdbcUrlForExecutor
        Map("query" -> s"select * from `${dbName}`.`${tableName}`",
          HWC_SESSION_ID_KEY -> sessionId,
          HWC_QUERY_MODE -> EXECUTE_HIVE_JDBC.name(),
          QueryExecutionUtil.HIVE_JDBC_URL_FOR_EXECUTOR -> jdbcUrlForExecutor)
      }

      val options = Map(HWC_SESSION_ID_KEY -> sessionId) ++
        tableOpts ++
        sessionOpts
      DataSourceV2Relation.create(ds, options, None, None)
  }
}

object HWCSwitchRule {
  private val hwcSessions: JMap[SparkSession, HiveWarehouseSessionImpl] =
    new JHashMap[SparkSession, HiveWarehouseSessionImpl]()
}
