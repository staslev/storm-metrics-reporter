package com.github.staslev.storm.metrics.yammer;

import com.yammer.metrics.core.Gauge;

public class SettableGauge<T> extends Gauge<T> {

    protected volatile T value;

    public SettableGauge(T value) {
        this.value = value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public T value() {
        return value;
    }
}


