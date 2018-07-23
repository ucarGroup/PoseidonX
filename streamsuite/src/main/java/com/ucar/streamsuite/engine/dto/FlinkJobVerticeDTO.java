package com.ucar.streamsuite.engine.dto;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.List;

/**
 * Description: flink job vertice 信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class FlinkJobVerticeDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;


    public FlinkJobVerticeDTO() {
    }

    private String id;
    private String name;
    private Integer parallelism;
    private String status;
    private Long startTime;
    private Long endTime;
    private Long duration;

    private String startTimeShow;
    private String uptime;

    //此字段不序列化。手工设置
    private transient List<FlinkJobVerticeSubTaskDTO> subTasks;

    @JSONField(name="id")
    public String getId() {
        return id;
    }
    @JSONField(name="id")
    public void setId(String id) {
        this.id = id;
    }
    @JSONField(name="name")
    public String getName() {
        return name;
    }
    @JSONField(name="name")
    public void setName(String name) {
        this.name = name;
    }
    @JSONField(name="parallelism")
    public Integer getParallelism() {
        return parallelism;
    }
    @JSONField(name="parallelism")
    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }
    @JSONField(name="status")
    public String getStatus() {
        return status;
    }
    @JSONField(name="status")
    public void setStatus(String status) {
        this.status = status;
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

    public List<FlinkJobVerticeSubTaskDTO> getSubTasks() {
        return subTasks;
    }

    public void setSubTasks(List<FlinkJobVerticeSubTaskDTO> subTasks) {
        this.subTasks = subTasks;
    }

    public String getStartTimeShow() {
        return startTimeShow;
    }

    public void setStartTimeShow(String startTimeShow) {
        this.startTimeShow = startTimeShow;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }
}
