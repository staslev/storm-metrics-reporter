package com.github.staslev.storm.metrics;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import java.util.Arrays;

public class Metric {

  public static class Option {

    public static final Predicate<Optional<Metric>> isPresent =
            new Predicate<Optional<Metric>>() {
              @Override
              public boolean apply(final Optional<Metric> metricOption) {
                return metricOption.isPresent();
              }
            };

    public static final Function<Optional<Metric>, Metric> getValue =
            new Function<Optional<Metric>, Metric>() {
              @Override
              public Metric apply(final Optional<Metric> metricOption) {
                return metricOption.get();
              }
            };
  }

  public static final String NAME_FRAGMENT_SEPARATOR = ".";

  private final String component;
  private final String operation;
  private final double value;

  public Metric(final String component, final String operation, final double value) {
    this.component = component;
    this.operation = operation;
    this.value = value;
  }

  /**
   * Joins multiple metric name strings using a Graphite style dot separator.
   *
   * @param metricNameFragments The metric metricNameFragments.
   * @return A joined metric name string.
   */
  public static String joinNameFragments(Object... metricNameFragments) {
    return Joiner.on(NAME_FRAGMENT_SEPARATOR).join(Arrays.asList(metricNameFragments));
  }

  /**
   * Removes restricted characters from a metric name fragment.
   *
   * @param metricNameFragment A metric name fragment string.
   * @return A "clean" metric name fragment string, with restricted characters replaced.
   */
  public static String cleanNameFragment(final String metricNameFragment) {
    return metricNameFragment
            .replace("__", "")
            .replace('.', '_')
            .replace('/', '.')
            .replace(':', '_');
  }

  public String getMetricName() {
    return joinNameFragments(getComponent(), getOperation());
  }

  public double getValue() {
    return value;
  }

  public String getComponent() {
    return component;
  }

  public String getOperation() {
    return operation;
  }
}
