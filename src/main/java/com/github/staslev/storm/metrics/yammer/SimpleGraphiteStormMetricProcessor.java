package com.github.staslev.storm.metrics.yammer;

import backtype.storm.metric.api.IMetricsConsumer;
import com.github.staslev.storm.metrics.Metric;
import com.github.staslev.storm.metrics.StormMetricProcessor;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.GraphiteReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

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

  public class SettableGauge<T> extends Gauge<T> {

    protected volatile T value;

    public SettableGauge(T value) {
      this.value = value;
    }

    public void setValue(T value) {
      this.value = value;
    }

    @Override
    public T value() {
      return value;
    }
  }

  public static final String REPORT_PERIOD_IN_SEC = "metric.reporter.graphite.report.period.sec";

  private static final int DEFAULT_REPORT_PERIOD_SEC = 30;

  public static final Logger LOG = LoggerFactory.getLogger(SimpleGraphiteStormMetricProcessor.class);

  private static MetricsRegistry metricsRegistry = new MetricsRegistry();

  @SuppressWarnings("FieldCanBeLocal")
  private final GraphiteReporter graphiteReporter;

  public SimpleGraphiteStormMetricProcessor(final String topologyName,
                                            final String graphiteHost,
                                            final Integer graphitePort,
                                            final Map config) {
    super(topologyName, graphiteHost, graphitePort, config);
    try {
      graphiteReporter = new GraphiteReporter(metricsRegistry,
                                              getMetricsServerHost(),
                                              getMetricsServerPort(),
                                              Metric.joinNameFragments("Storm", getTopologyName()));

      graphiteReporter.start(getGraphiteReportPeriod(config), TimeUnit.SECONDS);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int getGraphiteReportPeriod(final Map config) {
    return config.containsKey(REPORT_PERIOD_IN_SEC) ?
           Integer.parseInt(config.get(REPORT_PERIOD_IN_SEC).toString()) :
           DEFAULT_REPORT_PERIOD_SEC;
  }

  private SettableGauge<Double> createOrUpdateGauge(final Metric metric, final MetricName metricName) {
    final SettableGauge<Double> settableGauge =
            (SettableGauge<Double>) metricsRegistry.newGauge(metricName, new SettableGauge<>(metric.getValue()));
    settableGauge.setValue(metric.getValue());
    return settableGauge;
  }

  @Override
  public void process(final Metric metric, final IMetricsConsumer.TaskInfo taskInfo) {

    final MetricName metricName = new MetricName(Metric.joinNameFragments(taskInfo.srcWorkerHost,
                                                                          taskInfo.srcWorkerPort,
                                                                          metric.getComponent()),
                                                 Integer.toString(taskInfo.srcTaskId),
                                                 metric.getOperation());

    try {
      createOrUpdateGauge(metric, metricName);
    } catch (final Exception e) {
      LOG.error(String.format("Unable to process metric %s", metricName.toString()), e);
    }
  }
}
