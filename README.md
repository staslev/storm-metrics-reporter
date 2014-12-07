A pretty generic Storm metrics reporter, currently used to adapt Storm metrics to Yammer metrics API, inspired by [storm-metrics-statsd](https://github.com/endgameinc/storm-metrics-statsd/).


Usage
--------

Build the jar:

```bash 
git clone https://github.com/staslev/storm-metrics-reporter.git
cd storm-metrics-reporter
mvn package install
```
Add a pom dependency:

```xml
<dependency>
  <groupId>com.github.staslev</groupId>
  <artifactId>storm-metrics-reporter</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```
Configure your topology:

1.  Specify `MetricReporter` as the metric consumer.
2.  `YOUR_GRAPHITE_SERVER` and `YOUR_GRAPHITE_PORT` values. 
3.  Specify the metrics you want to report by providing an appropriate regular expression, in the example below we're reporting any metric that has the words "execute", "latency" or "capacity".
4.  Specify your `StormMetricGauge` implementation, in the example we're using the provided `SimpleGraphiteStormMetricGauge` class.

```java
final Config config = new Config();
config.put(MetricReporter.METRICS_HOST, YOUR_GRAPHITE_SERVER);
config.put(MetricReporter.METRICS_PORT, YOUR_GRAPHITE_PORT);
config.registerMetricsConsumer(MetricReporter.class,
                               new MetricReporterConfig("(.*execute.*|.*latency.*|.*capacity.*)",
                                                        SimpleGraphiteStormMetricGauge.class.getCanonicalName()),
                               1);
```

Screenshots
-----------

![Graphite metric hierarchy](https://raw.githubusercontent.com/staslev/storm-metrics-reporter/master/screenshots/graphite-metrics-hierarchy.png "Graphite metric hierarchy")

![Execute Latency Metrics](https://raw.githubusercontent.com/staslev/storm-metrics-reporter/master/screenshots/graphite-capacity-metrics.png "Execute latency metrics (not provided by Storm directly)")

Disclaimers
-----------
* This projects was inspired by [storm-metrics-statsd](https://github.com/endgameinc/storm-metrics-statsd/), thanks [jt6211](https://github.com/jt6211)!
* The screenshots above were generated using a custom `StormMetricGauge`, not the one included in the sources. It depicts a particular Graphite naming convention, which is subject to change in other Graphite environemnts.
* storm-metrics-reporter currently supports reporting metrics to Graphite only, but it should be pretty straight forward to extend it to support other metrics reporting mechanisms.
