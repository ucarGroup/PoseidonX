package com.ucar.streamsuite.cql.dto;

/**
 * Description: 过cql创建task记录
 * Created on 2018/3/14 上午11:03
 *
 */
public class JstormCqlTaskDTO {

    private String projectJarPath;
    private String taskName;
    private String taskCql;
    private String zkServers;
    private String zkPort;
    private String zkRoot;
    private String workers;
    private String workerGcOpts = "-XX:SurvivorRatio=4 -XX:MaxTenuringThreshold=20 -XX:+UseConcMarkSweepGC  -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=70 -XX:CMSFullGCsBeforeCompaction=5 -XX:+HeapDumpOnOutOfMemoryError -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseCMSCompactAtFullCollection -XX:CMSMaxAbortablePrecleanTime=5000";
    private String workerMemory = "1073741824";

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

    public String getZkServers() {
        return zkServers;
    }

    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }

    public String getZkPort() {
        return zkPort;
    }

    public void setZkPort(String zkPort) {
        this.zkPort = zkPort;
    }

    public String getZkRoot() {
        return zkRoot;
    }

    public void setZkRoot(String zkRoot) {
        this.zkRoot = zkRoot;
    }

    public String getWorkers() {
        return workers;
    }

    public void setWorkers(String workers) {
        this.workers = workers;
    }

    public String getWorkerGcOpts() {
        return workerGcOpts;
    }

    public void setWorkerGcOpts(String workerGcOpts) {
        this.workerGcOpts = workerGcOpts;
    }

    public String getWorkerMemory() {
        return workerMemory;
    }

    public void setWorkerMemory(String workerMemory) {
        this.workerMemory = workerMemory;
    }

    public String getProjectJarPath() {
        return projectJarPath;
    }

    public void setProjectJarPath(String projectJarPath) {
        this.projectJarPath = projectJarPath;
    }
}
