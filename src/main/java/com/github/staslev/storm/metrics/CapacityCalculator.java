package com.github.staslev.storm.metrics;

import backtype.storm.metric.api.IMetricsConsumer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * Calculates capacity metrics for Storm bolt components, based on the built-in execution count and latency metrics
 * provided by Storm.
 */
public class CapacityCalculator {

  private static final String EXECUTE_COUNT = "execute-count";
  private static final String EXECUTE_LATENCY = "execute-latency";
  private static final String CAPACITY = "execute-capacity";

  private static final Predicate<Metric> isExecuteCountMetric = new Predicate<Metric>() {
    @Override
    public boolean apply(final Metric metric) {
      return metric.getMetricName().contains(EXECUTE_COUNT);
    }
  };

  private static final Predicate<Metric> isExecuteLatencyMetric = new Predicate<Metric>() {
    @Override
    public boolean apply(final Metric metric) {
      return metric.getMetricName().contains(EXECUTE_LATENCY);
    }
  };

  private static Optional<Metric> calculateCapacityMetric(final String component,
                                                          final Optional<Metric> count,
                                                          final Optional<Metric> latency,
                                                          final int updateIntervalSecs) {
    if (count.isPresent() && latency.isPresent()) {

      final String executeOperation = count.get().getOperationAfterString(EXECUTE_COUNT);
      final String latencyOperation = latency.get().getOperationAfterString(EXECUTE_LATENCY);

      if (executeOperation.equals(latencyOperation)) {
        double capacity =
                count.get().getValue() * latency.get().getValue() / (updateIntervalSecs * 1000);
        return Optional.of(new Metric(component, Metric.joinNameFragments(CAPACITY, executeOperation), capacity));
      } else {
        return Optional.absent();
      }
    } else {
      return Optional.absent();
    }
  }

  /**
   * Goes over the specified metrics and if both execute-count and execute-latency are present,
   * computes the capacity metric according to the formula capacity = execute-count * execute-latency / time-window-ms
   *
   * @param component2metrics metrics keyed by component name.
   * @param taskInfo          additional task information pertaining to the reporting task.
   * @return The capacity metrics that were calculated based on the specified input metrics.
   */
  public static ImmutableList<Metric> calculateCapacityMetrics(final Map<String, List<Metric>> component2metrics,
                                                               final IMetricsConsumer.TaskInfo taskInfo) {

    final Function<Map.Entry<String, List<Metric>>, Optional<Metric>> toCapacityMetric =
            new Function<Map.Entry<String, List<Metric>>, Optional<Metric>>() {
              @Override
              public Optional<Metric> apply(final Map.Entry<String, List<Metric>> componentMetrics) {

                final String component = componentMetrics.getKey();
                final FluentIterable<Metric> metrics = FluentIterable.from(componentMetrics.getValue());
                final Optional<Metric> count = metrics.firstMatch(isExecuteCountMetric);
                final Optional<Metric> latency = metrics.firstMatch(isExecuteLatencyMetric);

                return calculateCapacityMetric(component, count, latency, taskInfo.updateIntervalSecs);
              }
            };

    return FluentIterable
            .from(component2metrics.entrySet())
            .transform(toCapacityMetric)
            .filter(Metric.Option.isPresent)
            .transform(Metric.Option.getValue)
            .toList();
  }
}
