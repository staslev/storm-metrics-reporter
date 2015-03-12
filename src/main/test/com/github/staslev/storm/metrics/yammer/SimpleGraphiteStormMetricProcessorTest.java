package com.github.staslev.storm.metrics.yammer;

import backtype.storm.Config;
import com.github.staslev.storm.metrics.MetricReporter;
import com.github.staslev.storm.metrics.MetricReporterConfig;
import com.github.staslev.storm.metrics.StormMetricProcessor;
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
    config.put(MetricReporter.METRICS_HOST, host);
    config.put(MetricReporter.METRICS_PORT, port);
    config.put(SimpleGraphiteStormMetricProcessor.REPORT_PERIOD_IN_SEC, reportPeriod);
    config.put(Config.TOPOLOGY_NAME, topologyName);

    final StormMetricProcessor stormMetricProcessor =
            new MetricReporterConfig(".*",
                                     SimpleGraphiteStormMetricProcessor.class.getCanonicalName())
                    .getStormMetricProcessor(config);

    assertThat(stormMetricProcessor.getMetricsServerHost(), is(host));
    assertThat(stormMetricProcessor.getMetricsServerPort(), is(port));
    assertThat(stormMetricProcessor.getTopologyName(), is(topologyName));
    assertThat(stormMetricProcessor.getConfig(), is(config));

  }


}