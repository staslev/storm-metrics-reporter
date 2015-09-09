package com.github.staslev.storm.metrics.yammer;

import com.github.staslev.storm.metrics.Metric;
import com.github.staslev.storm.metrics.StormMetricProcessor;
import com.yammer.metrics.reporting.GraphiteReporter;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SimpleGraphiteStormMetricProcessor extends SimpleStormMetricProcessor {

    public static final String REPORT_PERIOD_IN_SEC = "metric.reporter.graphite.report.period.sec";
    private static final int DEFAULT_REPORT_PERIOD_SEC = 30;

    public static final String GRAPHITE_HOST = "metric.reporter.graphite.report.host";
    private static final String DEFAULT_GRAPHITE_HOST = "localhost";

    public static final String GRAPHITE_PORT = "metric.reporter.graphite.report.port";
    private static final int DEFAULT_GRAPHITE_PORT = 2004;

    @SuppressWarnings("FieldCanBeLocal")
    private final GraphiteReporter graphiteReporter;

    public SimpleGraphiteStormMetricProcessor(final String topologyName,
                                              final Map config) {
        super(topologyName, config);

        try {
            graphiteReporter = new GraphiteReporter(StormMetricProcessor.METRICS_REGISTRY,
                    getGraphiteServerHost(config),
                    getGraphiteServerPort(config),
                    Metric.joinNameFragments("Storm", topologyName));

            graphiteReporter.start(getGraphiteReportPeriod(config), TimeUnit.SECONDS);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    String getGraphiteServerHost(final Map config) {
        return config.containsKey(GRAPHITE_HOST) ?
                config.get(GRAPHITE_HOST).toString() :
                DEFAULT_GRAPHITE_HOST;
    }

    int getGraphiteServerPort(final Map config) {
        return config.containsKey(GRAPHITE_PORT) ?
                Integer.parseInt(config.get(GRAPHITE_PORT).toString()) :
                DEFAULT_GRAPHITE_PORT;
    }

    int getGraphiteReportPeriod(final Map config) {
        return config.containsKey(REPORT_PERIOD_IN_SEC) ?
                Integer.parseInt(config.get(REPORT_PERIOD_IN_SEC).toString()) :
                DEFAULT_REPORT_PERIOD_SEC;
    }
}
