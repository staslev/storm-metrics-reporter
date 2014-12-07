package com.github.staslev.storm.metrics;

public class Metric {

  private final String component;
  private final String operation;
  private final double value;

  public Metric(final String component, final String operation, final double value) {
    this.component = component;
    this.operation = operation;
    this.value = value;
  }

  public double getValue() {
    return value;
  }

  public String getComponent() {
    return component;
  }

  public String getOperation() {
    return operation;
  }
}
