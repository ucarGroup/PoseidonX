package com.ucar.streamsuite.engine.dto;


import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;

/**
 * Description: flink job vertice 信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class FlinkJobVerticeSubTaskDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    public FlinkJobVerticeSubTaskDTO() {
    }

    private Integer subtask;
    private String status;
    private String host;
    private Long startTime;
    private Long endTime;
    private Long duration;

    @JSONField(name="subtask")
    public Integer getSubtask() {
        return subtask;
    }

    @JSONField(name="subtask")
    public void setSubtask(Integer subtask) {
        this.subtask = subtask;
    }

    @JSONField(name="status")
    public String getStatus() {
        return status;
    }

    @JSONField(name="status")
    public void setStatus(String status) {
        this.status = status;
    }

    @JSONField(name="host")
    public String getHost() {
        return host;
    }

    @JSONField(name="host")
    public void setHost(String host) {
        this.host = host;
    }

    @JSONField(name="start-time")
    public Long getStartTime() {
        return startTime;
    }

    @JSONField(name="start-time")
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    @JSONField(name="end-time")
    public Long getEndTime() {
        return endTime;
    }

    @JSONField(name="end-time")
    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    @JSONField(name="duration")
    public Long getDuration() {
        return duration;
    }

    @JSONField(name="duration")
    public void setDuration(Long duration) {
        this.duration = duration;
    }
}
