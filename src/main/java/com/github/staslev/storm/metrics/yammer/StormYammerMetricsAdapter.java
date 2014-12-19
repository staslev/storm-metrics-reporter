package com.github.staslev.storm.metrics.yammer;

import backtype.storm.task.TopologyContext;
import com.yammer.metrics.core.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StormYammerMetricsAdapter {

  private final MetricsRegistry metricsRegistry;

  private StormYammerMetricsAdapter(final Map stormConf,
                                    final TopologyContext context,
                                    final MetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
    YammerFacadeMetric.register(stormConf, context, this.metricsRegistry);
  }

  public static StormYammerMetricsAdapter configure(final Map stormConf,
                                                    final TopologyContext context,
                                                    final MetricsRegistry metricsRegistry) {
    return new StormYammerMetricsAdapter(stormConf, context, metricsRegistry);
  }

  private MetricName getMetricName(final String component, final String methodName) {
    return new MetricName("", component, methodName);
  }

  public Timer createTimer(final String component, final String methodName) {
    return metricsRegistry.newTimer(getMetricName(component, methodName), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  }

  public Counter createCounter(final String component, final String methodName) {
    return metricsRegistry.newCounter(getMetricName(component, methodName));
  }

  public <T> Gauge<T> createGauge(final String component, final String methodName, final Gauge<T> metric) {
    return metricsRegistry.newGauge(getMetricName(component, methodName), metric);
  }

  public Meter createMeter(final String component, final String methodName, final String eventType) {
    return metricsRegistry.newMeter(getMetricName(component, methodName), eventType, TimeUnit.SECONDS);
  }

  public Histogram createHistogram(final String component, final String methodName, final boolean biased) {
    return metricsRegistry.newHistogram(getMetricName(component, methodName), biased);
  }

}
