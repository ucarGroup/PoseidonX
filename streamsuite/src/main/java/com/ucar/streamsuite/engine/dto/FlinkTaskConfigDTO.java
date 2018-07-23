package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;

/**
 * Description: flink任务配置信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class FlinkTaskConfigDTO extends TaskConfigDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315880L;

    private String taskName;
    private Integer taskId;
    private String taskCql;
    private Integer taskCqlId;
    private String projectJarPath;
    private String classPath;
    private Integer taskMangerNum;
    private Long taskMangerMem;
    private Long jobMangerMem;
    private Integer slots;
    private Integer parallelism;

    private Integer submitType;
    //客户化参数
    private String customParams;

    public Integer getSlots() {
        return slots;
    }

    public void setSlots(Integer slots) {
        this.slots = slots;
    }

    public Long getTaskMangerMem() {
        return taskMangerMem;
    }

    public void setTaskMangerMem(Long taskMangerMem) {
        this.taskMangerMem = taskMangerMem;
    }

    public Long getJobMangerMem() {
        return jobMangerMem;
    }

    public void setJobMangerMem(Long jobMangerMem) {
        this.jobMangerMem = jobMangerMem;
    }

    public String getCustomParams() {
        return customParams;
    }

    public void setCustomParams(String customParams) {
        this.customParams = customParams;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getTaskCql() {
        return taskCql;
    }

    public void setTaskCql(String taskCql) {
        this.taskCql = taskCql;
    }

    public Integer getTaskCqlId() {
        return taskCqlId;
    }

    public void setTaskCqlId(Integer taskCqlId) {
        this.taskCqlId = taskCqlId;
    }

    public String getProjectJarPath() {
        return projectJarPath;
    }

    public void setProjectJarPath(String projectJarPath) {
        this.projectJarPath = projectJarPath;
    }

    public String getClassPath() {
        return classPath;
    }

    public void setClassPath(String classPath) {
        this.classPath = classPath;
    }

    public Integer getTaskMangerNum() {
        return taskMangerNum;
    }

    public void setTaskMangerNum(Integer taskMangerNum) {
        this.taskMangerNum = taskMangerNum;
    }

    public Integer getSubmitType() {
        return submitType;
    }

    public void setSubmitType(Integer submitType) {
        this.submitType = submitType;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }
}
