package com.github.staslev.storm.metrics.yammer;

import backtype.storm.metric.api.IMetricsConsumer;
import com.github.staslev.storm.metrics.Metric;
import com.github.staslev.storm.metrics.StormMetricProcessor;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.reporting.JmxReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.util.Map;

public class SimpleJMXStormMetricProcessor extends SimpleStormMetricProcessor {

    public static final Logger LOG = LoggerFactory.getLogger(SimpleJMXStormMetricProcessor.class);

    public SimpleJMXStormMetricProcessor(final Map config) {
        super(config);
        new JmxReporter(StormMetricProcessor.METRICS_REGISTRY).start();
        LOG.info("Metrics JMXReport started");
    }

    String mBeanName(Metric metric, IMetricsConsumer.TaskInfo taskInfo) {
        return "storm"
                + ":topology=" + topologyName
                + ",component=" + metric.getComponent()
                + ",operation=" + ObjectName.quote(metric.getOperation())
                + ",host-port-task=" + String.format("%s-%s-%s", taskInfo.srcWorkerHost
                    ,taskInfo.srcWorkerPort
                    ,taskInfo.srcTaskId);
    }

    @Override
    public MetricName name(Metric metric, IMetricsConsumer.TaskInfo taskInfo) {
        return new MetricName("storm",
                topologyName,
                metric.getComponent(),
                metric.getOperation(),
                mBeanName(metric, taskInfo));
    }
}
