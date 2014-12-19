package com.github.staslev.storm.metrics.yammer;

import backtype.storm.task.TopologyContext;
import com.yammer.metrics.core.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An adapter between the Yammer metrics API and the Storm metrics mechanism.
 * This class allows one to operate on standard Yammer metrics API, while under the hood these metrics are transported
 * using Storm's metrics API.
 */
public class StormYammerMetricsAdapter {

  private final MetricsRegistry metricsRegistry;

  private StormYammerMetricsAdapter(final Map stormConf,
                                    final TopologyContext context,
                                    final MetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
    YammerFacadeMetric.register(stormConf, context, this.metricsRegistry);
  }

  private MetricName getMetricName(final String component, final String methodName) {
    return new MetricName("", component, methodName);
  }

  /**
   * Constructs a <code>StormYammerMetricsAdapter</code> instance.
   * <br/><br/>
   *
   * Note: <code>configure</code> should NOT be called more than once in the scope of a given
   * Storm component (bolt/spout).
   *
   * @param stormConf Storm configuration settings.
   * @param context TopologyContext for the topology a face metric is to be reporting metrics for.
   * @param metricsRegistry A metric registry instance where underlying metrics are to be stored.
   * @return A <code>StormYammerMetricsAdapter</code> instance.
   */
  public static StormYammerMetricsAdapter configure(final Map stormConf,
                                                    final TopologyContext context,
                                                    final MetricsRegistry metricsRegistry) {
    return new StormYammerMetricsAdapter(stormConf, context, metricsRegistry);
  }

  /**
   * See {@link com.yammer.metrics.core.MetricsRegistry#newTimer}
   */
  public Timer createTimer(final String component, final String methodName) {
    return metricsRegistry.newTimer(getMetricName(component, methodName), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  }

  /**
   * See {@link com.yammer.metrics.core.MetricsRegistry#newCounter}
   */
  public Counter createCounter(final String component, final String methodName) {
    return metricsRegistry.newCounter(getMetricName(component, methodName));
  }

  /**
   * See {@link com.yammer.metrics.core.MetricsRegistry#newGauge}
   */
  public <T> Gauge<T> createGauge(final String component, final String methodName, final Gauge<T> metric) {
    return metricsRegistry.newGauge(getMetricName(component, methodName), metric);
  }

  /**
   * See {@link com.yammer.metrics.core.MetricsRegistry#newMeter}
   */
  public Meter createMeter(final String component, final String methodName, final String eventType) {
    return metricsRegistry.newMeter(getMetricName(component, methodName), eventType, TimeUnit.SECONDS);
  }

  /**
   * See {@link com.yammer.metrics.core.MetricsRegistry#newHistogram}
   */
  public Histogram createHistogram(final String component, final String methodName, final boolean biased) {
    return metricsRegistry.newHistogram(getMetricName(component, methodName), biased);
  }

}
