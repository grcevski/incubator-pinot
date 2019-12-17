/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.metrics;

import com.yammer.metrics.core.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertNotNull;
import static org.apache.pinot.common.utils.CommonConstants.Server.DEFAULT_METRICS_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the MetricsHelper class.
 *
 */
public class MetricsHelperTest {
  public static boolean listenerOneOkay;
  public static boolean listenerTwoOkay;

  public static class ListenerOne implements MetricsRegistryRegistrationListener {
    @Override
    public void onMetricsRegistryRegistered(MetricsRegistry metricsRegistry) {
      listenerOneOkay = true;
    }
  }

  public static class ListenerTwo implements MetricsRegistryRegistrationListener {
    @Override
    public void onMetricsRegistryRegistered(MetricsRegistry metricsRegistry) {
      listenerTwoOkay = true;
    }
  }

  @Test
  public void testMetricsHelperRegistration() {
    listenerOneOkay = false;
    listenerTwoOkay = false;

    Map<String, String> configKeys = new HashMap<String, String>();
    configKeys.put("pinot.broker.metrics.metricsRegistryRegistrationListeners",
        ListenerOne.class.getName() + "," + ListenerTwo.class.getName());
    Configuration configuration = new MapConfiguration(configKeys);

    MetricsRegistry registry = new MetricsRegistry();

    // Initialize the MetricsHelper and create a new timer
    MetricsHelper.initializeMetrics(configuration.subset("pinot.broker.metrics"));
    MetricsHelper.registerMetricsRegistry(registry);
    MetricsHelper.newTimer(registry, new MetricName(MetricsHelperTest.class, "dummy"), TimeUnit.MILLISECONDS,
        TimeUnit.MILLISECONDS);

    // Check that the two listeners fired
    assertTrue(listenerOneOkay);
    assertTrue(listenerTwoOkay);
  }

  @Test
  public void testMetricNameGetHelper() {
    final String METRIC_NAME = "dummy";

    MetricName newlyAllocated = new MetricName(ServerMetrics.class, METRIC_NAME);
    AbstractMetrics metrics = new ServerMetrics(new MetricsRegistry());
    MetricName fromHelper = metrics.getMetricName(METRIC_NAME);
    // Direct metrics creation must match what the helper does
    assertEquals(fromHelper, newlyAllocated);
    // Make sure we have a different object here, we only used the helper once
    assertTrue(newlyAllocated != fromHelper);
    MetricName fromHelperCached = metrics.getMetricName(METRIC_NAME);
    // Identity test has to work because we are caching the names
    assertTrue(fromHelper == fromHelperCached);
  }

  @Test
  public void testMetricsApisTiming() {
    final String TABLE_NAME = "customers";

    MetricsRegistry registry = new MetricsRegistry();
    AbstractMetrics metrics = new ServerMetrics(registry);

    metrics.addPhaseTiming(TABLE_NAME, BrokerQueryPhase.QUERY_EXECUTION, 10);
    metrics.addPhaseTiming(TABLE_NAME, BrokerQueryPhase.QUERY_EXECUTION, 15);

    Map<MetricName, Metric> allMetrics = registry.allMetrics();
    assertTrue(allMetrics.size() == 1);
    Timer timer = (Timer)allMetrics.values().iterator().next();
    // The sum should be the two query executions added together
    assertEquals(timer.sum(), 25.0/1_000_000);
  }

  @Test
  public void testMetricsApisMeteredTableValues() {
    final String TABLE_NAME = "customers";

    MetricsRegistry registry = new MetricsRegistry();
    AbstractMetrics metrics = new ServerMetrics(registry);

    metrics.addMeteredTableValue(TABLE_NAME, ServerMeter.NUM_SEGMENTS_MATCHED, 10);
    Meter meter = metrics.getMeteredTableValue(TABLE_NAME, ServerMeter.NUM_SEGMENTS_MATCHED);

    assertNotNull(meter);
  }

  @Test
  public void testMetricsApisMeteredQueryValues() {
    final String TABLE_NAME = "customers";

    MetricsRegistry registry = new MetricsRegistry();
    AbstractMetrics metrics = new ServerMetrics(registry);

    metrics.addMeteredQueryValue(null, ServerMeter.NUM_SEGMENTS_MATCHED, 10);

    Map<MetricName, Metric> allMetrics = registry.allMetrics();
    assertTrue(allMetrics.size() == 1);
  }

  @Test
  public void testMetricsCallBackGauge() {
    final String METRIC_NAME = "test";

    MetricsRegistry registry = new MetricsRegistry();
    AbstractMetrics metrics = new ServerMetrics(registry);

    metrics.addCallbackGauge(METRIC_NAME, () -> ServerGauge.DOCUMENT_COUNT);

    Map<MetricName, Metric> allMetrics = registry.allMetrics();
    assertTrue(allMetrics.size() == 1);
    MetricName fromHelper = metrics.getMetricName(DEFAULT_METRICS_PREFIX + METRIC_NAME);
    // Identity test has to work because we are caching the names
    assertTrue(fromHelper == allMetrics.keySet().iterator().next());
  }
}
