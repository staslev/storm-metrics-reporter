package com.github.staslev.storm.metrics.yammer;

import com.github.staslev.storm.metrics.Metric;
import com.github.staslev.storm.metrics.StormMetricProcessor;
import com.yammer.metrics.core.MetricName;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
public class SimpleStormMetricProcessor implements StormMetricProcessor {

    public static final Logger LOG = LoggerFactory.getLogger(SimpleStormMetricProcessor.class);

    private static final String DEFAULT_TOPOLOGY_NAME = "default";

    final protected String topologyName;
    final protected Map config;

    public SimpleStormMetricProcessor(final Map config) {
        this.config = config;
        this.topologyName = config.containsKey(Config.TOPOLOGY_NAME) ?
                        config.get(Config.TOPOLOGY_NAME).toString() :
                        DEFAULT_TOPOLOGY_NAME;
    }

    private SettableGauge<Double> createOrUpdateGauge(final Metric metric, final MetricName metricName) {
        final SettableGauge<Double> settableGauge =
                (SettableGauge<Double>) METRICS_REGISTRY.newGauge(metricName, new SettableGauge(metric.getValue()));
        settableGauge.setValue(metric.getValue());
        return settableGauge;
    }

    @Override
    public MetricName name(final Metric metric, final IMetricsConsumer.TaskInfo taskInfo) {
        return new MetricName(Metric.joinNameFragments(taskInfo.srcWorkerHost,
                        taskInfo.srcWorkerPort,
                        metric.getComponent()),
                        Integer.toString(taskInfo.srcTaskId),
                        metric.getOperation());
    }

    @Override
    public void process(final Metric metric, final IMetricsConsumer.TaskInfo taskInfo) {

        final MetricName metricName = name(metric, taskInfo);
        try {
            createOrUpdateGauge(metric, metricName);
        } catch (final Exception e) {
            LOG.error(String.format("Unable to process metric %s", metricName.toString()), e);
        }
    }
}
