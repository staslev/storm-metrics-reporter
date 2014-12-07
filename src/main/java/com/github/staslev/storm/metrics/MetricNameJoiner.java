package com.github.staslev.storm.metrics;

import com.google.common.base.Joiner;

import java.util.Arrays;

public class MetricNameJoiner {

  /**
   * Joins multiple metric name strings using a Graphite style dot separator.
   *
   * @param metricNames The metric metricNames.
   * @return A joined metric name string.
   */
  public static String join(Object... metricNames) {
    return Joiner.on(".").join(Arrays.asList(metricNames));
  }

}
