package com.github.staslev.storm.metrics.yammer;

import backtype.storm.Config;
import com.github.staslev.storm.metrics.MetricReporterConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SimpleGraphiteStormMetricProcessorTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testStormMetricProcessorCreationViaReflection() throws Exception {

    final String host = "someHost";
    final int port = 1234;
    final int reportPeriod = 13;
    final String topologyName = "someTopology";

    Map config = new HashMap();
    config.put(YammerFacadeMetric.FACADE_METRIC_TIME_BUCKET_IN_SEC, 60);
    config.put(SimpleGraphiteStormMetricProcessor.GRAPHITE_HOST, host);
    config.put(SimpleGraphiteStormMetricProcessor.GRAPHITE_PORT, port);
    config.put(SimpleGraphiteStormMetricProcessor.REPORT_PERIOD_IN_SEC, reportPeriod);
    config.put(Config.TOPOLOGY_NAME, topologyName);


    MetricReporterConfig metricReporterConfig =
            new MetricReporterConfig(".*", SimpleGraphiteStormMetricProcessor.class.getCanonicalName());
    final SimpleGraphiteStormMetricProcessor stormMetricProcessor =
            (SimpleGraphiteStormMetricProcessor)metricReporterConfig.getStormMetricProcessor(config);

    assertThat(stormMetricProcessor.getGraphiteServerHost(config), is(host));
    assertThat(stormMetricProcessor.getGraphiteServerPort(config), is(port));
    assertThat(stormMetricProcessor.topologyName, is(topologyName));
    assertThat(stormMetricProcessor.config, is(config));

  }
}