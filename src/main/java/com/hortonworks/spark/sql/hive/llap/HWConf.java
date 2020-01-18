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

import java.util.Map;
import java.util.Optional;

import org.apache.spark.sql.RuntimeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.SparkSession;

import static java.lang.String.format;

/**
 * See: {@link org.apache.spark.sql.sources.v2.SessionConfigSupport}
 */
public enum HWConf {

  //ENUM(shortKey, qualifiedKey, default)
  USER("user.name", warehouseKey("user.name"), ""),
  PASSWORD("password", warehouseKey("password"), ""),
  RESOLVED_HS2_URL("hs2.url.resolved", warehouseKey("hs2.url.resolved"), ""),
  DBCP2_CONF("dbcp2.conf", warehouseKey("dbcp2.conf"), null),
  DEFAULT_DB("default.db", warehouseKey("default.db"), "default"),
  MAX_EXEC_RESULTS("exec.results.max", warehouseKey("exec.results.max"), 0),
  LOAD_STAGING_DIR("load.staging.dir", warehouseKey("load.staging.dir"), "/tmp"),
  ARROW_ALLOCATOR_MAX("arrow.allocator.max", warehouseKey("arrow.allocator.max"), Long.MAX_VALUE),
  COUNT_TASKS("count.tasks", warehouseKey("count.tasks"), 100),
  SMART_EXECUTION("smartExecution", warehouseKey("smartExecution"), "false"),
  //on batch write path, setting this ensures that dataframe has columns congruent to hive table
  WRITE_PATH_STRICT_COLUMN_NAMES_MAPPING("write.path.strictColumnNamesMapping",
      warehouseKey("write.path.strictColumnNamesMapping"), "true"),
  READ_VIA_LLAP("read.via.llap", warehouseKey("read.via.llap"), "false"),
  READ_JDBC_MODE("read.jdbc.mode", warehouseKey("read.jdbc.mode"), "client"),
  ;

  private HWConf(String simpleKey, String qualifiedKey, Object defaultValue) {
    this.simpleKey = simpleKey;
    this.qualifiedKey = qualifiedKey;
    this.defaultValue = defaultValue;
  }

  static String warehouseKey(String keySuffix) {
    return HiveWarehouseSession.CONF_PREFIX + "." + keySuffix;
  }

  private static Logger LOG = LoggerFactory.getLogger(HWConf.class);
  public static final String HIVESERVER2_CREDENTIAL_ENABLED = "spark.security.credentials.hiveserver2.enabled";
  public static final String HIVESERVER2_JDBC_URL_PRINCIPAL = "spark.sql.hive.hiveserver2.jdbc.url.principal";
  public static final String HIVESERVER2_JDBC_URL = "spark.sql.hive.hiveserver2.jdbc.url";
  //possible values - client/cluster. default - client
  public static final String SPARK_SUBMIT_DEPLOYMODE = "spark.submit.deployMode";
  public static final String SPARK_MASTER = "spark.master";
  public static final String PARTITION_OPTION_KEY = "partition";
  public static final String HWC_EXECUTION_MODE = "spark.sql.hive.hwc.execution.mode";
  public static final String HWC_EXECUTION_MODE_LLAP = "llap";
  public static final String HWC_EXECUTION_MODE_SPARK = "spark";

  public String getQualifiedKey() {
    return qualifiedKey;
  }

  public void setString(HiveWarehouseSessionState state, String value) {
    state.props.put(qualifiedKey, value);
    state.session.sessionState().conf().setConfString(qualifiedKey, value);
  }

  public void setInt(HiveWarehouseSessionState state, Integer value) {
    state.props.put(qualifiedKey, Integer.toString(value));
    state.session.sessionState().conf().setConfString(qualifiedKey, Integer.toString(value));
  }

  //This is called from executors so it can't depend explicitly on session state
  public String getFromOptionsMap(Map<String, String> options) {
    String value = options.get(simpleKey);
    return Optional.ofNullable(value != null ? value : options.get(simpleKey.toLowerCase()))
        .orElse(defaultValue == null ? null : defaultValue.toString());
  }

  public static String getHwcExecutionMode(HiveWarehouseSessionState state) {
    if (state.session.conf().contains(HWC_EXECUTION_MODE)) {
      return state.session.conf().get(HWC_EXECUTION_MODE);
    }
    return HWC_EXECUTION_MODE_LLAP;
  }

  public static String getSparkSqlExtension(HiveWarehouseSessionState state) {
    if (state.session.conf().contains("spark.sql.extensions")) {
      return state.session.conf().get("spark.sql.extensions");
    }
    return "";
  }

  String getString(HiveWarehouseSessionState state) {
    return Optional.
      ofNullable((String) state.props.get(qualifiedKey)).
      orElse(state.session.sessionState().conf().getConfString(
        qualifiedKey, (String) defaultValue)
      );
  }

  Integer getInt(HiveWarehouseSessionState state) {
    return Integer.parseInt(
      Optional.
        ofNullable(state.props.get(qualifiedKey)).
        orElse(state.session.sessionState().conf().getConfString(
        qualifiedKey, defaultValue.toString())
      )
      );
  }

  String simpleKey;
  String qualifiedKey;
  Object defaultValue;
  /**
   * Return connection URL (with replaced proxy user name if exists).
   */
  public static String getConnectionUrl(HiveWarehouseSessionState state) {
    String userString = USER.getString(state);
    if (userString == null) {
      userString = "";
}
    String urlString = getConnectionUrlFromConf(state);
    String returnValue = urlString.replace("${user}", userString);
    LOG.info("Using HS2 URL: {}", returnValue);
    return returnValue;
  }

  /**
   * For the given HiveServer2 JDBC URLs, attach the postfix strings if needed.
   *
   * For kerberized clusters,
   *
   * 1. YARN cluster mode: ";auth=delegationToken"
   * 2. YARN client mode: ";principal=hive/_HOST@EXAMPLE.COM"
   *
   * Non-kerberied clusters,
   * 3. Use the given URLs.
   */
   public static String getConnectionUrlFromConf(HiveWarehouseSessionState state) {
     SparkSession sparkSession = state.session;
     RuntimeConfig conf = sparkSession.conf();
     if ((conf.contains(HIVESERVER2_CREDENTIAL_ENABLED) && conf.get(HIVESERVER2_CREDENTIAL_ENABLED).equals("true"))
         || conf.contains(HIVESERVER2_JDBC_URL_PRINCIPAL)) {
       String deployMode = conf.get(SPARK_SUBMIT_DEPLOYMODE, "client");
       LOG.debug("Getting jdbc connection url for kerberized cluster with spark.submit.deployMode = {}", deployMode);
       if (deployMode.equals("cluster")) {
         // 1. YARN Cluster mode for kerberized clusters
         return format("%s;auth=delegationToken", conf.get(HIVESERVER2_JDBC_URL));
       } else {
         // 2. YARN Client mode for kerberized clusters
         return format("%s;principal=%s",
             conf.get(HIVESERVER2_JDBC_URL),
             conf.get(HIVESERVER2_JDBC_URL_PRINCIPAL));
       }
     } else {
       LOG.debug("Getting jdbc connection url for non-kerberized cluster");
       // 3. For non-kerberized cluster
       return conf.get(HIVESERVER2_JDBC_URL);
     }
   }

}
