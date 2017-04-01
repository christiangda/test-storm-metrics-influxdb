package com.github.christiangda.storm.metrics;

import org.apache.storm.metric.api.IMetric;

public class TupleRateMetric implements IMetric {

    double recived;
    int window;

    public TupleRateMetric(int window) {
        this.window = window;
    }

    public void incrRecived() {
        this.recived++;
    }

    @Override
    public Object getValueAndReset() {
        double rate = (recived / window);
        recived = 0;
        return rate;
    }
}
