package com.github.staslev.storm.metrics;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A metric consumer implementation that reports storm metrics to Graphite.
 * The metrics to be reported are specified using a regular expression, so as to avoid burdening Graphite with
 * undesired metrics.
 * <br/>
 * This metric consumer also reports a capacity metric, computed for each taskId based on the number of executions
 * and per-execution latency reported by Storm internals.
 * <br/><br/><url>Inspired by <url>https://github.com/endgameinc/storm-metrics-statsd</url>
 */
public class MetricReporter implements IMetricsConsumer {

  public static final String METRICS_HOST = "metric.reporter.host";
  public static final String METRICS_PORT = "metric.reporter.port";

  private static final String EXECUTE_COUNT = "execute-count";
  private static final String EXECUTE_LATENCY = "execute-latency";
  private static final String CAPACITY = "execute-capacity";

  private static final Predicate<Metric> isExecuteCountMetric = new Predicate<Metric>() {
    @Override
    public boolean apply(final Metric metric) {
      return metricName(metric).contains(EXECUTE_COUNT);
    }
  };


  private static final Predicate<Metric> isExecuteLatencyMetric = new Predicate<Metric>() {
    @Override
    public boolean apply(final Metric metric) {
      return metricName(metric).contains(EXECUTE_LATENCY);
    }
  };

  private static final Predicate<Optional<Metric>> isOptionSuccess = new Predicate<Optional<Metric>>() {
    @Override
    public boolean apply(final Optional<Metric> metricOption) {
      return metricOption.isPresent();
    }
  };

  private static final Function<Optional<Metric>, Metric> getOptionValue = new Function<Optional<Metric>, Metric>() {
    @Override
    public Metric apply(final Optional<Metric> metricOption) {
      return metricOption.get();
    }
  };

  private MetricMatcher allowedMetrics;
  private StormMetricGauge stormMetricGauge;

  private static String metricName(final Metric metric) {
    return metric.getComponent() + "." + metric.getOperation();
  }

  private String clean(final String string) {
    return string
            .replace("__", "")
            .replace('.', '_')
            .replace('/', '.')
            .replace(':', '_');
  }

  private Optional<Metric> getMetricOption(final String component,
                                           final Optional<Metric> count,
                                           final Optional<Metric> latency,
                                           final TaskInfo taskInfo) {
    if (count.isPresent() && latency.isPresent()) {

      final String executeOperation =
              count.get()
                   .getOperation()
                   .substring(count.get()
                                   .getOperation()
                                   .indexOf(EXECUTE_COUNT) + EXECUTE_COUNT.length() + 1);

      final String latencyOperation =
              latency.get().getOperation()
                     .substring(latency.get()
                                       .getOperation()
                                       .indexOf(EXECUTE_LATENCY) + EXECUTE_LATENCY.length() + 1);

      if (executeOperation.equals(latencyOperation)) {
        double capacity =
                count.get().getValue() * latency.get().getValue() / (taskInfo.updateIntervalSecs * 1000);
        return Optional.of(new Metric(component, MetricNameJoiner.join(CAPACITY, executeOperation), capacity));
      } else {
        return Optional.absent();
      }
    } else {
      return Optional.absent();
    }
  }

  private Map<String, List<Metric>> toMetricsByComponent(final Collection<DataPoint> dataPoints,
                                                         final TaskInfo taskInfo) {

    final Map<String, List<Metric>> component2metrics = Maps.newHashMap();

    for (final DataPoint dataPoint : dataPoints) {
      final String component = clean(taskInfo.srcComponentId);

      if (!component2metrics.containsKey(component)) {
        component2metrics.put(component, new LinkedList<Metric>());
      }

      if (dataPoint.value instanceof Number) {
        component2metrics.get(component).add(new Metric(component,
                                                        clean(dataPoint.name),
                                                        value(dataPoint.value)));
      } else if (dataPoint.value instanceof Map) {
        @SuppressWarnings("rawtypes")
        final Map map = (Map) dataPoint.value;
        for (final Object subName : map.keySet()) {
          final Object subValue = map.get(subName);
          if (subValue instanceof Number) {
            component2metrics.get(component).add(new Metric(component,
                                                            clean(dataPoint.name) + "." + clean(subName.toString()),
                                                            value(subValue)));
          }
        }
      }
    }

    return component2metrics;
  }

  private double value(final Object value) {
    return ((Number) value).doubleValue();
  }

  /**
   * Goes over the specified metrics and if both execute-count and execute-latency are present,
   * computes the capacity metric according to the formula capacity = execute-count * execute-latency / time-window-ms
   *
   * @param component2metrics metrics keyed by component name.
   * @param taskInfo          additional task information pertaining to the reporting task.
   * @return The capacity metrics that were computed based on the specified input metrics.
   */
  private ImmutableList<Metric> computeCapacityMetrics(final Map<String, List<Metric>> component2metrics,
                                                       final TaskInfo taskInfo) {

    final Function<Map.Entry<String, List<Metric>>, Optional<Metric>> toCapacityMetric =
            new Function<Map.Entry<String, List<Metric>>, Optional<Metric>>() {
              @Override
              public Optional<Metric> apply(final Map.Entry<String, List<Metric>> componentMetrics) {

                final FluentIterable<Metric> metrics = FluentIterable.from(componentMetrics.getValue());
                final Optional<Metric> count = metrics.firstMatch(isExecuteCountMetric);
                final Optional<Metric> latency = metrics.firstMatch(isExecuteLatencyMetric);

                return getMetricOption(componentMetrics.getKey(), count, latency, taskInfo);
              }
            };

    return FluentIterable
            .from(component2metrics.entrySet())
            .transform(toCapacityMetric)
            .filter(isOptionSuccess)
            .transform(getOptionValue)
            .toList();
  }

  @Override
  public void prepare(final Map stormConf,
                      final Object registrationArgument,
                      final TopologyContext context,
                      final IErrorReporter errorReporter) {

    final MetricReporterConfig config = MetricReporterConfig.from((List<String>) registrationArgument);
    allowedMetrics = new MetricMatcher(config.getAllowedMetricNames());
    stormMetricGauge = config.getStormMetricGauge((String) stormConf.get(Config.TOPOLOGY_NAME),
                                                  (String) stormConf.get(METRICS_HOST),
                                                  Integer.parseInt(stormConf.get(METRICS_PORT).toString()));

  }

  @Override
  public void handleDataPoints(final TaskInfo taskInfo, final Collection<DataPoint> dataPoints) {

    final Map<String, List<Metric>> component2metrics = toMetricsByComponent(dataPoints, taskInfo);
    final ImmutableList<Metric> capacityMetrics = computeCapacityMetrics(component2metrics, taskInfo);
    final Iterable<Metric> providedMetrics = Iterables.concat(component2metrics.values());
    final Iterable<Metric> allMetrics = Iterables.concat(providedMetrics, capacityMetrics);

    for (final Metric metric : FluentIterable.from(allMetrics).filter(allowedMetrics).toList()) {
      stormMetricGauge.report(metric, taskInfo);
    }
  }

  @Override
  public void cleanup() {
  }
}