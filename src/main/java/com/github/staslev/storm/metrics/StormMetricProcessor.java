package com.github.staslev.storm.metrics;

import backtype.storm.metric.api.IMetricsConsumer;

/**
 * Responsible for processing a metric reported by Storm.
 * <br/><br/>
 * NOTE: The implementing class must take into account that the reporting granularity is taskId,
 * that is, it should make sure it does not overwrite values by aggregating incoming value incorrectly (for instance,
 * aggregating per workerHost-port-componentId is wrong, since the various tasks might overwrite each other's values.
 */
public abstract class StormMetricProcessor {

  private final String topologyName;
  private final String metricsServerHost;
  private final int metricsServerPort;

  protected StormMetricProcessor(final String topologyName,
                                 final String metricsServerHost,
                                 final Integer metricsServerPort) {
    this.topologyName = topologyName;
    this.metricsServerHost = metricsServerHost;
    this.metricsServerPort = metricsServerPort;
  }

  public String getTopologyName() {
    return topologyName;
  }

  public String getMetricsServerHost() {
    return metricsServerHost;
  }

  public int getMetricsServerPort() {
    return metricsServerPort;
  }

  public abstract void process(final Metric metric, final IMetricsConsumer.TaskInfo taskInfo);
}
