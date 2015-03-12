package com.github.staslev.storm.metrics.yammer;

import backtype.storm.metric.api.IMetricsConsumer;
import com.github.staslev.storm.metrics.Metric;
import com.github.staslev.storm.metrics.StormMetricProcessor;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.reporting.GraphiteReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A straight forward {@link StormMetricProcessor} implementation that reports values according to the following
 * metric hierarchy:
 * <pre>
 *   -Storm
 *    -WorkerHost
 *      -WorkerPort
 *        -ComponentName
 *          -TaskId
 *            -OperationName
 *              -value
 * </pre>
 * <p/>
 * Aggregations (e.g., stats per component) are assumed to be the back-end's responsibility in this case.
 * <p/>
 * <br/>Client might want to implement a custom StormMetricGauge in order to employ the metric naming convention
 * that fits them best. This implementation is more of a showcase.
 */
public class SimpleGraphiteStormMetricProcessor extends StormMetricProcessor {

  public static final Logger LOG = LoggerFactory.getLogger(SimpleGraphiteStormMetricProcessor.class);

  private final GraphiteReporter graphiteReporter;

  public SimpleGraphiteStormMetricProcessor(final String topologyName,
                                            final String graphiteHost,
                                            final Integer graphitePort) {
    super(topologyName, graphiteHost, graphitePort);
    try {
      graphiteReporter = new GraphiteReporter(getMetricsServerHost(),
                                              getMetricsServerPort(),
                                              Metric.joinNameFragments("Storm", getTopologyName()));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(final Metric metric, final IMetricsConsumer.TaskInfo taskInfo) {

    final MetricName metricName = new MetricName(Metric.joinNameFragments(taskInfo.srcWorkerHost,
                                                                          taskInfo.srcWorkerPort,
                                                                          metric.getComponent()),
                                                 Integer.toString(taskInfo.srcTaskId),
                                                 metric.getOperation());

    final Gauge<Double> gauge = new Gauge<Double>() {
      @Override
      public Double value() {
        return metric.getValue();
      }
    };

    try {
      graphiteReporter.processGauge(metricName, gauge, taskInfo.timestamp);
    } catch (final Exception e) {
      final String msg = String.format("Unable to send metric %s to Graphite server %s:%d",
                                       metricName.toString(),
                                       getMetricsServerHost(),
                                       getMetricsServerPort());
      LOG.error(msg, e);
    }
  }
}
