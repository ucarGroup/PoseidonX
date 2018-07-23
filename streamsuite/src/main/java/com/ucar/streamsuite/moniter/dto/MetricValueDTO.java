package com.ucar.streamsuite.moniter.dto;


import java.io.Serializable;

/**
 * Description: MetricValueDTO
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class MetricValueDTO implements Serializable{

    private static final long serialVersionUID = -5729044331250315860L;

    /**
     * 时间
     */
    private long time;

    /**
     * 指标数据
     */
    private String metricValue;

    public MetricValueDTO(long time,String metricValue) {
        this.time = time;
        this.metricValue = metricValue;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getMetricValue() {
        return metricValue;
    }

    public void setMetricValue(String metricValue) {
        this.metricValue = metricValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricValueDTO that = (MetricValueDTO) o;

        return time == that.time;
    }

    @Override
    public int hashCode() {
        return (int) (time ^ (time >>> 32));
    }
}


