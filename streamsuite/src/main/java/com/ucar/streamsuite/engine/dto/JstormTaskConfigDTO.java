package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;
import java.util.List;

/**
 * Description: jstorm 任务配置信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class JstormTaskConfigDTO extends TaskConfigDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315880L;

    private String blotNum;
    private String spoutNum;
    private Integer workerNum;
    private Long workerMem;
    private List<String> jstormZkHost;
    private Integer jstormZkPort;
    private String jstormZkRoot;
    private String taskName;
    private Integer taskId;
    private Integer jstormJarId;
    private Integer yarnAmJarId;
    private String jstormJarPath;
    private String yarnAmJarPath;
    private Integer submitType;
    private String classPath;
    private String projectJarPath;
    private String taskCql;
    private Integer taskCqlId;

    public String getTaskCql() {
        return taskCql;
    }

    public void setTaskCql(String taskCql) {
        this.taskCql = taskCql;
    }

    public String getBlotNum() {
        return blotNum;
    }

    public void setBlotNum(String blotNum) {
        this.blotNum = blotNum;
    }

    public String getSpoutNum() {
        return spoutNum;
    }

    public void setSpoutNum(String spoutNum) {
        this.spoutNum = spoutNum;
    }

    public Integer getWorkerNum() {
        return workerNum;
    }

    public void setWorkerNum(Integer workerNum) {
        this.workerNum = workerNum;
    }

    public Long getWorkerMem() {
        return workerMem;
    }

    public void setWorkerMem(Long workerMem) {
        this.workerMem = workerMem;
    }

    public String getProjectJarPath() {
        return projectJarPath;
    }

    public void setProjectJarPath(String projectJarPath) {
        this.projectJarPath = projectJarPath;
    }

    public String getJstormJarPath() {
        return jstormJarPath;
    }

    public void setJstormJarPath(String jstormJarPath) {
        this.jstormJarPath = jstormJarPath;
    }

    public String getYarnAmJarPath() {
        return yarnAmJarPath;
    }

    public void setYarnAmJarPath(String yarnAmJarPath) {
        this.yarnAmJarPath = yarnAmJarPath;
    }

    public String getClassPath() {
        return classPath;
    }

    public void setClassPath(String classPath) {
        this.classPath = classPath;
    }

    public List<String> getJstormZkHost() {
        return jstormZkHost;
    }

    public void setJstormZkHost(List<String> jstormZkHost) {
        this.jstormZkHost = jstormZkHost;
    }

    public Integer getJstormZkPort() {
        return jstormZkPort;
    }

    public void setJstormZkPort(Integer jstormZkPort) {
        this.jstormZkPort = jstormZkPort;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public Integer getJstormJarId() {
        return jstormJarId;
    }

    public void setJstormJarId(Integer jstormJarId) {
        this.jstormJarId = jstormJarId;
    }

    public Integer getYarnAmJarId() {
        return yarnAmJarId;
    }

    public void setYarnAmJarId(Integer yarnAmJarId) {
        this.yarnAmJarId = yarnAmJarId;
    }

    public String getJstormZkRoot() {
        return jstormZkRoot;
    }

    public void setJstormZkRoot(String jstormZkRoot) {
        this.jstormZkRoot = jstormZkRoot;
    }

    public Integer getSubmitType() {
        return submitType;
    }

    public void setSubmitType(Integer submitType) {
        this.submitType = submitType;
    }

    public Integer getTaskCqlId() {
        return taskCqlId;
    }

    public void setTaskCqlId(Integer taskCqlId) {
        this.taskCqlId = taskCqlId;
    }
}
