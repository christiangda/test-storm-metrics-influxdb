package com.github.christiangda.storm.metrics;

import org.apache.storm.metric.api.IMetric;

/**
 * Created by christian on 12/19/16.
 */
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
