package com.ucar.streamsuite.engine.dto;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.List;

/**
 * Description: flink job 信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class FlinkJobDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    private transient String appId;
    private transient String appStatus;

    private transient Integer slotsTotal;
    private transient Integer slotsAvailable;
    private transient String flinkVersion;
    private transient Integer taskmanagers;
    private transient String startTimeShow;
    private transient String uptime;

    private String jid;
    private String name;
    private String state;
    private Long startTime;
    private Long duration;


    private List<FlinkJobVerticeDTO> vertices;

    public FlinkJobDTO() {
    }

    @JSONField(name="jid")
    public String getJid() {
        return jid;
    }

    @JSONField(name="jid")
    public void setJid(String jid) {
        this.jid = jid;
    }

    @JSONField(name="name")
    public String getName() {
        return name;
    }

    @JSONField(name="name")
    public void setName(String name) {
        this.name = name;
    }

    @JSONField(name="state")
    public String getState() {
        return state;
    }

    @JSONField(name="state")
    public void setState(String state) {
        this.state = state;
    }

    @JSONField(name="start-time")
    public Long getStartTime() {
        return startTime;
    }

    @JSONField(name="start-time")
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    @JSONField(name="duration")
    public Long getDuration() {
        return duration;
    }

    @JSONField(name="duration")
    public void setDuration(Long duration) {
        this.duration = duration;
    }

    @JSONField(name="vertices")
    public List<FlinkJobVerticeDTO> getVertices() {
        return vertices;
    }

    @JSONField(name="vertices")
    public void setVertices(List<FlinkJobVerticeDTO> vertices) {
        this.vertices = vertices;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppStatus() {
        return appStatus;
    }

    public void setAppStatus(String appStatus) {
        this.appStatus = appStatus;
    }

    public Integer getSlotsTotal() {
        return slotsTotal;
    }

    public void setSlotsTotal(Integer slotsTotal) {
        this.slotsTotal = slotsTotal;
    }

    public Integer getSlotsAvailable() {
        return slotsAvailable;
    }

    public void setSlotsAvailable(Integer slotsAvailable) {
        this.slotsAvailable = slotsAvailable;
    }

    public String getFlinkVersion() {
        return flinkVersion;
    }

    public void setFlinkVersion(String flinkVersion) {
        this.flinkVersion = flinkVersion;
    }

    public Integer getTaskmanagers() {
        return taskmanagers;
    }

    public void setTaskmanagers(Integer taskmanagers) {
        this.taskmanagers = taskmanagers;
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
