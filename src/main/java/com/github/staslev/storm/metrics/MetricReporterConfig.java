package com.github.staslev.storm.metrics;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Holds configuration options for the {@link StormMetricProcessor} metric consumer.
 * Implements List in order to be compliant with Storm's configuration serialization mechanism,
 * while exposing type safe getters.
 */
public class MetricReporterConfig extends ArrayList<String> {

  public MetricReporterConfig(final String allowedMetricNames, final String stormMetricProcessorClassName) {
    super(2);
    add(allowedMetricNames);
    add(stormMetricProcessorClassName);
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
   * @param stormConf configuration parameters
   * @return A new GaugeReporter instance of the specified class.
   */
  public StormMetricProcessor getStormMetricProcessor(final Map stormConf) {
    try {
      final Constructor<?> constructor = Class.forName(getStormMetricProcessorClassName()).getConstructor(Map.class);
      return (StormMetricProcessor) constructor.newInstance(stormConf);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
