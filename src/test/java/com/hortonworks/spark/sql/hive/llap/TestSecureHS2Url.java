package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.common.HWConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSecureHS2Url extends SessionTestBase {

  static final String TEST_HS2_URL = "jdbc:hive2://example.com:10084";
  static final String TEST_PRINCIPAL = "testUser/_HOST@EXAMPLE.com";

  static final String KERBERIZED_CLUSTER_MODE_URL = TEST_HS2_URL + ";auth=delegationToken";
  static final String KERBERIZED_CLIENT_MODE_URL =
      TEST_HS2_URL +
          ";principal=" +
          TEST_PRINCIPAL;

  static final String HIVE_CONF_LIST =
      "hive.vectorized.execution.filesink.arrow.native.enabled=true;hive.vectorized.execution.enabled=true";

  @Test
  public void kerberizedClusterMode() {
    session.conf().set(HWConf.SPARK_SUBMIT_DEPLOYMODE, "cluster");
    HiveWarehouseSessionState state = HiveWarehouseBuilder
        .session(session)
        .hs2url(TEST_HS2_URL)
        .credentialsEnabled()
        .build()
        .sessionState;

    String resolvedUrl = HWConf.RESOLVED_HS2_URL.getString(state);
    assertEquals(resolvedUrl, KERBERIZED_CLUSTER_MODE_URL);
  }

  @Test
  public void kerberizedClientMode() {
    session.conf().set("spark.security.credentials.hiveserver2.enabled", "false");
    HiveWarehouseSessionState state = HiveWarehouseBuilder
        .session(session)
        .hs2url(TEST_HS2_URL)
        .principal(TEST_PRINCIPAL)
        .build()
        .sessionState;

    String resolvedUrl = HWConf.RESOLVED_HS2_URL.getString(state);
    assertEquals(resolvedUrl, KERBERIZED_CLIENT_MODE_URL);
  }

  @Test
  public void nonKerberized() {
    session.conf().set("spark.security.credentials.hiveserver2.enabled", "false");
    HiveWarehouseSessionState state = HiveWarehouseBuilder
        .session(session)
        .hs2url(TEST_HS2_URL)
        .build()
        .sessionState;

    String resolvedUrl = HWConf.RESOLVED_HS2_URL.getString(state);
    assertEquals(resolvedUrl, TEST_HS2_URL);
  }

  @Test
  public void nonKerberizedWithConfList() {
    session.conf().set(HWConf.HIVESERVER2_JDBC_URL_CONF_LIST, HIVE_CONF_LIST);
    session.conf().set(HWConf.HIVESERVER2_CREDENTIAL_ENABLED, "false");
    verifyResolvedUrl(TEST_HS2_URL + "?" + HIVE_CONF_LIST);
  }

  @Test
  public void kerberizedClientModeWithConfList() {
    session.conf().set(HWConf.HIVESERVER2_JDBC_URL_CONF_LIST, HIVE_CONF_LIST);
    session.conf().set(HWConf.HIVESERVER2_CREDENTIAL_ENABLED, "true");
    session.conf().set(HWConf.HIVESERVER2_JDBC_URL_PRINCIPAL, TEST_PRINCIPAL);
    verifyResolvedUrl(KERBERIZED_CLIENT_MODE_URL + "?" + HIVE_CONF_LIST);
  }

  @Test
  public void kerberizedClusterModeWithConfList() {
    session.conf().set(HWConf.SPARK_SUBMIT_DEPLOYMODE, "cluster");
    session.conf().set(HWConf.HIVESERVER2_JDBC_URL_CONF_LIST, HIVE_CONF_LIST);
    session.conf().set(HWConf.HIVESERVER2_CREDENTIAL_ENABLED, "true");

    verifyResolvedUrl(KERBERIZED_CLUSTER_MODE_URL + "?" + HIVE_CONF_LIST);
  }


  private void verifyResolvedUrl(String expectedUrl) {
    HiveWarehouseSessionState state = HiveWarehouseBuilder
        .session(session)
        .hs2url(TEST_HS2_URL)
        .build()
        .sessionState;

    String resolvedUrl = HWConf.RESOLVED_HS2_URL.getString(state);
    assertEquals(resolvedUrl, expectedUrl);
  }

}
