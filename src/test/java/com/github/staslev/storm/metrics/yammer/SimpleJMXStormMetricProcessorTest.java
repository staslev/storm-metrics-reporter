package com.github.staslev.storm.metrics.yammer;

import com.github.staslev.storm.metrics.Metric;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.junit.Test;

import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SimpleJMXStormMetricProcessorTest {

  SimpleJMXStormMetricProcessor processor;

  @Test
  public void testValidJMXObjectName() throws Exception {

    final String topologyName = "someTopology";

    Map config = new HashMap();
    config.put(Config.TOPOLOGY_NAME, topologyName);
    processor = new SimpleJMXStormMetricProcessor(config);

    Metric metric = new Metric("component", "kafkaPartition{host=kafka_9092, partition=0}", 1.9);
    IMetricsConsumer.TaskInfo taskInfo = new IMetricsConsumer.TaskInfo("localhost", 1010, "emitBot", 2, System.currentTimeMillis(), 100);

    String name = processor.mBeanName(metric, taskInfo);
    ObjectName objName = new ObjectName(name);

    assertThat(objName.getCanonicalName(), is("storm:component=component,host-port-task=localhost-1010-2,operation=\"kafkaPartition{host=kafka_9092, partition=0}\",topology=someTopology"));
  }


}