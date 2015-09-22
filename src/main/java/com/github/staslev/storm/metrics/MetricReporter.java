package com.github.staslev.storm.metrics;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import com.google.common.collect.*;

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

  private MetricMatcher allowedMetrics;
  private StormMetricProcessor stormMetricProcessor;

  private double value(final Object value) {
    return ((Number) value).doubleValue();
  }

  private Map<String, List<Metric>> toMetricsByComponent(final Collection<DataPoint> dataPoints,
                                                         final TaskInfo taskInfo) {

    final Map<String, List<Metric>> component2metrics = Maps.newHashMap();

    for (final DataPoint dataPoint : dataPoints) {
      final String component = Metric.cleanNameFragment(taskInfo.srcComponentId);

      if (!component2metrics.containsKey(component)) {
        component2metrics.put(component, new LinkedList<Metric>());
      }

      component2metrics.get(component).addAll(extractMetrics(dataPoint, component));
    }

    return component2metrics;
  }

  private List<Metric> extractMetrics(final DataPoint dataPoint, final String component) {

    List<Metric> metrics = Lists.newArrayList();

    if (dataPoint.value instanceof Number) {
      metrics.add(new Metric(component, Metric.cleanNameFragment(dataPoint.name), value(dataPoint.value)));
    } else if (dataPoint.value instanceof Map) {
      @SuppressWarnings("rawtypes")
      final Map map = (Map) dataPoint.value;
      for (final Object subName : map.keySet()) {
        final Object subValue = map.get(subName);
        if (subValue instanceof Number) {
          metrics.add(new Metric(component,
                                 Metric.joinNameFragments(Metric.cleanNameFragment(dataPoint.name),
                                                          Metric.cleanNameFragment(subName.toString())),
                                 value(subValue)));
        } else if (subValue instanceof Map) {
          metrics.addAll(extractMetrics(new DataPoint(Metric.joinNameFragments(dataPoint.name, subName), subValue),
                                        component));
        }
      }
    }

    return metrics;
  }

  @Override
  public void prepare(final Map stormConf,
                      final Object registrationArgument,
                      final TopologyContext context,
                      final IErrorReporter errorReporter) {

    @SuppressWarnings("unchecked")
    final MetricReporterConfig config = MetricReporterConfig.from((List<String>) registrationArgument);
    allowedMetrics = new MetricMatcher(config.getAllowedMetricNames());
    stormMetricProcessor = config.getStormMetricProcessor(stormConf);
  }

  @Override
  public void handleDataPoints(final TaskInfo taskInfo, final Collection<DataPoint> dataPoints) {

    final Map<String, List<Metric>> component2metrics = toMetricsByComponent(dataPoints, taskInfo);
    final ImmutableList<Metric> capacityMetrics = CapacityCalculator.calculateCapacityMetrics(component2metrics,
                                                                                              taskInfo);
    final Iterable<Metric> providedMetrics = Iterables.concat(component2metrics.values());
    final Iterable<Metric> allMetrics = Iterables.concat(providedMetrics, capacityMetrics);

    for (final Metric metric : FluentIterable.from(allMetrics).filter(allowedMetrics).toList()) {
      stormMetricProcessor.process(metric, taskInfo);
    }
  }

  @Override
  public void cleanup() {
  }
}