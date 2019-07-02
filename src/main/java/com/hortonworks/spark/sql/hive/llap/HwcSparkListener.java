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

import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HwcSparkListener - Handles various events throughout the lifecycle of spark application.
 * See {@link org.apache.spark.scheduler.SparkListener} for other events.
 * <p>
 * If we need to send some custom events, we can do it by pushing them to sparkContext().listenerBus()
 */
public class HwcSparkListener extends SparkListener {

  private static Logger LOG = LoggerFactory.getLogger(HwcSparkListener.class);

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    super.onApplicationEnd(applicationEnd);
    LOG.info("Spark onApplicationEnd event triggered, closing all llap resources");
    // close any resource held by application.
    // these resources may be jdbc connections, locks on table etc
    try {
      LlapBaseInputFormat.closeAll();
    } catch (Throwable e) {
      LOG.warn("Unable to close LlapBaseInputFormat resources cleanly. This may or may not indicate errors.", e);
    }
  }
}
