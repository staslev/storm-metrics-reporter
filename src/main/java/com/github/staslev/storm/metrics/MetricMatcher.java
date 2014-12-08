package com.github.staslev.storm.metrics;

import com.google.common.base.Predicate;

import java.util.regex.Pattern;

/**
 * Matches a metric full name (component and operation) according to a specified regular expression.
 * Implements the <code>Predicate</code> interface in order to be compliant with Guava.
 */
public class MetricMatcher implements Predicate<Metric> {

  private final Pattern compile;

  MetricMatcher(final String metricsRegex) {
    compile = Pattern.compile(metricsRegex);
  }

  private boolean match(final Metric metric) {
    return compile
            .matcher(metric.getMetricName())
            .matches();
  }

  @Override
  public boolean apply(final Metric metric) {
    return match(metric);
  }
}
