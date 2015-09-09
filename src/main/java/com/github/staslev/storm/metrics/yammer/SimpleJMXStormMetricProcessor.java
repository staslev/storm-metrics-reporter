package com.github.staslev.storm.metrics.yammer;

import backtype.storm.metric.api.IMetricsConsumer;
import com.github.staslev.storm.metrics.Metric;
import com.github.staslev.storm.metrics.StormMetricProcessor;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.reporting.JmxReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SimpleJMXStormMetricProcessor extends SimpleStormMetricProcessor {

    public static final Logger LOG = LoggerFactory.getLogger(SimpleJMXStormMetricProcessor.class);

    public SimpleJMXStormMetricProcessor(final String topologyName,
                                         final Map config) {
        super(topologyName, config);
        new JmxReporter(StormMetricProcessor.METRICS_REGISTRY).start();
        LOG.info("Metrics JMXReport started");
    }

    String mBeanName(String topology, Metric metric, IMetricsConsumer.TaskInfo taskInfo) {
        return "storm"
                + ":topology=" + topology
                + ",component=" + metric.getComponent()
                + ",operation=" + metric.getOperation()
                + ",host-port-task=" + String.format("%s-%s-%s", taskInfo.srcWorkerHost
                    ,taskInfo.srcWorkerPort
                    ,taskInfo.srcTaskId);
    }

    @Override
    public MetricName name(String topology, Metric metric, IMetricsConsumer.TaskInfo taskInfo) {
        return new MetricName("storm",
                topologyName,
                metric.getComponent(),
                metric.getOperation(),
                mBeanName(topology, metric, taskInfo));
    }
}
