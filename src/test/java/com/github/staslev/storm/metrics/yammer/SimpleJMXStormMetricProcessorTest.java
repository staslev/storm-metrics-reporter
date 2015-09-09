package com.github.staslev.storm.metrics.yammer;

import backtype.storm.metric.api.IMetricsConsumer;
import com.github.staslev.storm.metrics.Metric;
import com.google.common.collect.Maps;
import org.junit.Test;

import javax.management.ObjectName;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SimpleJMXStormMetricProcessorTest {

  SimpleJMXStormMetricProcessor processor;

  @Test
  public void testValidJMXObjectName() throws Exception {

    processor = new SimpleJMXStormMetricProcessor("testTopology", Maps.newHashMap());

    Metric metric = new Metric("component", "op", 1.9);
    IMetricsConsumer.TaskInfo taskInfo = new IMetricsConsumer.TaskInfo("localhost", 1010, "emitBot", 2, System.currentTimeMillis(), 100);

    String name = processor.mBeanName("testTopology", metric, taskInfo);
    ObjectName objName = new ObjectName(name);

    assertThat(objName.getCanonicalName(), is("storm:component=component,host-port-task=localhost-1010-2,operation=op,topology=testTopology"));
  }


}