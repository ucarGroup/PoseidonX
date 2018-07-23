package com.ucar.streamsuite.moniter.dto;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Description: 基于线的报表的dto
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class LineReportTimelineDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    /**
     * 时间线分组名称
     */
    private String group;

    /**
     * 时间线的数据
     */
    private LinkedList<MetricValueDTO> metricValues = new LinkedList<MetricValueDTO>();

    /**
     * 最近一条数据
     */
    private MetricValueDTO lastMetric;

    /**
     * 时间线最大长度(默认为2)
     */
    private int timeLineMaxSize = 2;

    public LineReportTimelineDTO() {
    }

    public LineReportTimelineDTO(String group) {
        this.group = group;
    }

    public LineReportTimelineDTO(String group,int timeLineMaxSize) {
        this.group = group;
        this.timeLineMaxSize = timeLineMaxSize;
    }

    public void append(MetricValueDTO timelineData){
       if(metricValues.size() < timeLineMaxSize){
           metricValues.addLast(timelineData);
           lastMetric = timelineData;
       }else{
           metricValues.removeFirst();
           metricValues.addLast(timelineData);
           lastMetric = timelineData;
       }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LineReportTimelineDTO that = (LineReportTimelineDTO) o;

        return group.equals(that.group);
    }

    @Override
    public int hashCode() {
        return group.hashCode();
    }

    public String getGroup() {
        return group;
    }

    public LinkedList<MetricValueDTO> getMetricValues() {
        return metricValues;
    }

    public void setMetricValues(LinkedList<MetricValueDTO> metricValues) {
        this.metricValues = metricValues;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getTimeLineMaxSize() {
        return timeLineMaxSize;
    }

    public void setTimeLineMaxSize(int timeLineMaxSize) {
        this.timeLineMaxSize = timeLineMaxSize;
    }

    public MetricValueDTO getLastMetric() {
        return lastMetric;
    }

    public void setLastMetric(MetricValueDTO lastMetric) {
        this.lastMetric = lastMetric;
    }
}
