package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;
import java.util.List;

/**
 * Description: flink任务运行时详情信息
 * Created on 2018/1/18 下午4:33
 *
 */
public class FlinkTaskDetailDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315880L;

    private String savepointTime;

    private Integer savepointType;

    private String appOverview;

    private List<String> taskManagers;

    private String jobmanagerConfig;

    private String jobDetail;

    private String exceptions;

    private List<String> vertices;

    public Integer getSavepointType() {
        return savepointType;
    }

    public void setSavepointType(Integer savepointType) {
        this.savepointType = savepointType;
    }

    public String getAppOverview() {
        return appOverview;
    }

    public void setAppOverview(String appOverview) {
        this.appOverview = appOverview;
    }

    public List<String> getTaskManagers() {
        return taskManagers;
    }

    public void setTaskManagers(List<String> taskManagers) {
        this.taskManagers = taskManagers;
    }

    public String getJobmanagerConfig() {
        return jobmanagerConfig;
    }

    public void setJobmanagerConfig(String jobmanagerConfig) {
        this.jobmanagerConfig = jobmanagerConfig;
    }

    public String getJobDetail() {
        return jobDetail;
    }

    public void setJobDetail(String jobDetail) {
        this.jobDetail = jobDetail;
    }

    public String getExceptions() {
        return exceptions;
    }

    public void setExceptions(String exceptions) {
        this.exceptions = exceptions;
    }

    public List<String> getVertices() {
        return vertices;
    }

    public void setVertices(List<String> vertices) {
        this.vertices = vertices;
    }

    public String getSavepointTime() {
        return savepointTime;
    }

    public void setSavepointTime(String savepointTime) {
        this.savepointTime = savepointTime;
    }
}
