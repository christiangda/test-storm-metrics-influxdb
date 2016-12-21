package com.github.christiangda.storm.metrics;

import org.apache.storm.metric.api.IMetric;

public class SuccessRateMetric implements IMetric {

    double success;
    double fail;

    public void incrSuccess() {
        success++;
    }

    public void incrFail() {
        fail++;
    }

    @Override
    public Object getValueAndReset() {
        double rate = (success / (success + fail)) * 100.0;
        success = 0;
        fail = 0;
        return rate;
    }
}
