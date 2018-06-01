package com.iiot.stream.bean;

import com.htiiot.store.model.Metric;

public class MetricImpl extends Metric{
    private Long metricId;
    //yuzhi
    private String threshold;

    public MetricImpl(String name, double value) {
        super(name,value);
    }

    public Long getMetricId() {
        return metricId;
    }

    public void setMetricId(Long metricId) {
        this.metricId = metricId;
    }

    public String getThreshold() {
        return threshold;
    }

    public void setThreshold(String threshold) {
        this.threshold = threshold;
    }
}
