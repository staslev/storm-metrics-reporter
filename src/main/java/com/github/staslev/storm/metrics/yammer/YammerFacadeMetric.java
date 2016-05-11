package com.github.staslev.storm.metrics.yammer;

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.TopologyContext;

import java.util.HashMap;
import java.util.Map;

/**
 * A metric facade, exposed to Storm's metric mechanism as a single metric, yet in fact holds multiple Yammer metrics.
 * Upon request to get it's value, it returns a Map representing all the underlying metrics it manages.
 */
public class YammerFacadeMetric implements IMetric {

  private static class MetricSerializer implements MetricProcessor<Map> {

    private String toString(final MetricName metricName) {
      return com.github.staslev.storm.metrics.Metric.joinNameFragments(metricName.getGroup(),
                                                                       metricName.getType(),
                                                                       metricName.getName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processMeter(final MetricName name, final Metered meter, final Map context) throws Exception {

      final Map subMetrics =
              ImmutableMap
                      .builder()
                      .put("count", meter.count())
                      .put("meanRate", meter.meanRate())
                      .put("1MinuteRate", meter.oneMinuteRate())
                      .put("5MinuteRate", meter.fiveMinuteRate())
                      .put("15MinuteRate", meter.fifteenMinuteRate())
                      .build();

      context.put(toString(name), subMetrics);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processCounter(final MetricName name, final Counter counter, final Map context) throws Exception {
      context.put(toString(name), counter.count());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processHistogram(final MetricName metricName,
                                 final Histogram histogram,
                                 final Map context) throws Exception {

      final Snapshot snapshot = histogram.getSnapshot();

      final Map subMetrics =
              ImmutableMap
                      .builder()
                      .put("75percentile", snapshot.get75thPercentile())
                      .put("95percentile", snapshot.get95thPercentile())
                      .put("99percentile", snapshot.get99thPercentile())
                      .put("median", snapshot.getMedian())
                      .put("mean", histogram.mean())
                      .put("min", histogram.min())
                      .put("max", histogram.max())
                      .put("stddev", histogram.stdDev())
                      .build();


      context.put(toString(metricName), subMetrics);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processTimer(final MetricName name, final Timer timer, final Map context) throws Exception {

      final Snapshot snapshot = timer.getSnapshot();

      final Map subMetrics =
              ImmutableMap
                      .builder()
                      .put("count", timer.count())
                      .put("median", snapshot.getMedian())
                      .put("75percentile", snapshot.get75thPercentile())
                      .put("95percentile", snapshot.get95thPercentile())
                      .put("99percentile", snapshot.get99thPercentile())
                      .build();

      context.put(toString(name), subMetrics);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processGauge(final MetricName name,
                             final com.yammer.metrics.core.Gauge<?> gauge,
                             final Map context) throws Exception {
      context.put(toString(name), gauge.value());
    }
  }

  public static final String FACADE_METRIC_TIME_BUCKET_IN_SEC = "metrics.reporter.yammer.facade..metric.bucket.seconds";
  public static final String FACADE_METRIC_NAME = "YammerFacadeMetric";
  private static final MetricSerializer METRIC_SERIALIZER = new MetricSerializer();
  private final MetricsRegistry metricsRegistry;

  private YammerFacadeMetric(final MetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
  }

  /**
   * Registers a facade metric with a given topology (represented by a {@link TopologyContext}).
   * with a {@link TopologyContext}.
   * <br/><br/>

   * <p/>
   * Multiple registrations might cause metric duplications and problems in the reporting flow.
   *
   * @param stormConf       Storm configuration settings.
   * @param context         TopologyContext for the topology a face metric is to be reporting metrics for.
   * @param metricsRegistry A metric registry instance where underlying metrics are to be stored.
   */
  public static void register(final Map stormConf,
                              final TopologyContext context,
                              final MetricsRegistry metricsRegistry) {

    context.registerMetric(FACADE_METRIC_NAME,
                           new YammerFacadeMetric(metricsRegistry),
                           Integer.parseInt(stormConf.get(FACADE_METRIC_TIME_BUCKET_IN_SEC).toString()));
  }

  /**
   * Returns a Map representing all the Yammer metrics managed by this facade metric.
   *
   * @return A Map which is in fact a snapshot of all the Yammer metrics managed by this facade metric.
   */
  @Override
  public Object getValueAndReset() {

    final Map metricsValues = new HashMap();

    for (final Map.Entry<MetricName, Metric> entry : metricsRegistry.allMetrics().entrySet()) {
      try {
        entry.getValue().processWith(METRIC_SERIALIZER, entry.getKey(), metricsValues);
      } catch (final Exception e) {
        // log?
      }
    }

    return metricsValues;
  }
}
