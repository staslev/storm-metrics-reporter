package com.github.staslev.storm.metrics;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds configuration options for the {@link StormMetricProcessor} metric consumer.
 * Implements List in order to be compliant with Storm's configuration serialization mechanism,
 * while exposing type safe getters.
 */
public class MetricReporterConfig extends ArrayList<String> {

  public MetricReporterConfig(final String allowedMetricNames, final String stormMetricGaugeClassName) {
    super(2);
    add(allowedMetricNames);
    add(stormMetricGaugeClassName);
  }

  public static MetricReporterConfig from(final List<String> params) {
    return new MetricReporterConfig(params.get(0), params.get(1));
  }

  public String getAllowedMetricNames() {
    return get(0);
  }

  public String getStormMetricProcessorClassName() {
    return get(1);
  }

  /**
   * Creates an instance of the configured {@link StormMetricProcessor} class.
   *
   * @param topologyName The name of the topology the newly created gauge will be reporting metrics for.
   * @return A new GaugeReporter instance of the specified class.
   */
  public StormMetricProcessor getStormMetricProcessor(final String topologyName,
                                                      final String metricsServerHost,
                                                      final int metricsServerPort) {
    try {
      final Constructor<?> constructor = Class.forName(getStormMetricProcessorClassName()).getConstructor(String.class,
                                                                                                      String.class,
                                                                                                      Integer.class);
      return (StormMetricProcessor) constructor.newInstance(topologyName, metricsServerHost, metricsServerPort);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
