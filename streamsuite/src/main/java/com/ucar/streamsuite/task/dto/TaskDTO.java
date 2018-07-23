package com.ucar.streamsuite.task.dto;

import java.util.Date;

/**
 * Description: 任务信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class TaskDTO {

    private static final long serialVersionUID = -5729044331250315790L;

    private Integer id;
    private String taskName;
    private Integer taskType;
    private Integer taskStatus;
    private Integer auditStatus;
    private String errorInfo;
    private String archiveId;
    private String archiveVersionId;
    private String remark;
    private String creatorUserName;
    private String auditUserName;
    private String modifyUserName;
    private Date modifyTime;
    private Date auditTime;
    private Date createTime;
    private Date taskStartTime;
    private Date taskStopTime;
    private Integer engineType;
    private Integer processId;
    private String blotNum;
    private String spoutNum;

    private String zkHosts;
    private Integer zkPort;
    private String workerMem;
    private String workerNum;
    private String slots;
    private String parallelism;

    private String  classPath;
    private String jstormEngineVersionId;
    private String yarnAmEngineVersionId;
    private Integer isCql;
    private String taskCql;
    private String taskCqlId;
    //客户化参数
    private String customParams;

    //以下为显示字段
    private String archiveShow;
    private String archiveVersionShow;
    private String jstormEngineVersionShow;
    private String yarnAmEngineVersionShow;
    private String zkAddressShow;
    private String engineTypeShow;
    private String taskTypeShow;
    private String taskStatusShow;
    private String auditStatusShow;
    private String taskCqlShow;
    private String yarnAddress;

    public String getSlots() {
        return slots;
    }

    public void setSlots(String slots) {
        this.slots = slots;
    }

    public String getCustomParams() {
        return customParams;
    }

    public void setCustomParams(String customParams) {
        this.customParams = customParams;
    }

    public Integer getProcessId() {
        return processId;
    }

    public void setProcessId(Integer processId) {
        this.processId = processId;
    }

    public String getYarnAddress() {
        return yarnAddress;
    }

    public void setYarnAddress(String yarnAddress) {
        this.yarnAddress = yarnAddress;
    }

    public String getJstormEngineVersionId() {
        return jstormEngineVersionId;
    }

    public void setJstormEngineVersionId(String jstormEngineVersionId) {
        this.jstormEngineVersionId = jstormEngineVersionId;
    }

    public String getYarnAmEngineVersionId() {
        return yarnAmEngineVersionId;
    }

    public void setYarnAmEngineVersionId(String yarnAmEngineVersionId) {
        this.yarnAmEngineVersionId = yarnAmEngineVersionId;
    }

    public String getTaskCqlShow() {
        return taskCqlShow;
    }

    public void setTaskCqlShow(String taskCqlShow) {
        this.taskCqlShow = taskCqlShow;
    }

    public String getArchiveShow() {
        return archiveShow;
    }

    public void setArchiveShow(String archiveShow) {
        this.archiveShow = archiveShow;
    }

    public String getArchiveVersionShow() {
        return archiveVersionShow;
    }

    public void setArchiveVersionShow(String archiveVersionShow) {
        this.archiveVersionShow = archiveVersionShow;
    }

    public String getJstormEngineVersionShow() {
        return jstormEngineVersionShow;
    }

    public void setJstormEngineVersionShow(String jstormEngineVersionShow) {
        this.jstormEngineVersionShow = jstormEngineVersionShow;
    }

    public String getYarnAmEngineVersionShow() {
        return yarnAmEngineVersionShow;
    }

    public void setYarnAmEngineVersionShow(String yarnAmEngineVersionShow) {
        this.yarnAmEngineVersionShow = yarnAmEngineVersionShow;
    }

    public String getZkAddressShow() {
        return zkAddressShow;
    }

    public void setZkAddressShow(String zkAddressShow) {
        this.zkAddressShow = zkAddressShow;
    }

    public String getTaskTypeShow() {
        return taskTypeShow;
    }

    public void setTaskTypeShow(String taskTypeShow) {
        this.taskTypeShow = taskTypeShow;
    }

    public String getTaskStatusShow() {
        return taskStatusShow;
    }

    public void setTaskStatusShow(String taskStatusShow) {
        this.taskStatusShow = taskStatusShow;
    }

    public String getAuditStatusShow() {
        return auditStatusShow;
    }

    public void setAuditStatusShow(String auditStatusShow) {
        this.auditStatusShow = auditStatusShow;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public Integer getTaskType() {
        return taskType;
    }

    public void setTaskType(Integer taskType) {
        this.taskType = taskType;
    }

    public String getClassPath() {
        return classPath;
    }

    public void setClassPath(String classPath) {
        this.classPath = classPath;
    }


    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getWorkerMem() {
        return workerMem;
    }

    public void setWorkerMem(String workerMem) {
        this.workerMem = workerMem;
    }

    public String getWorkerNum() {
        return workerNum;
    }

    public void setWorkerNum(String workerNum) {
        this.workerNum = workerNum;
    }

    public String getZkHosts() {
        return zkHosts;
    }

    public Integer getZkPort() {
        return zkPort;
    }

    public void setZkPort(Integer zkPort) {
        this.zkPort = zkPort;
    }

    public void setZkHosts(String zkHosts) {
        this.zkHosts = zkHosts;
    }

    public Integer getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(Integer taskStatus) {
        this.taskStatus = taskStatus;
    }

    public Date getAuditTime() {
        return auditTime;
    }

    public void setAuditTime(Date auditTime) {
        this.auditTime = auditTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getTaskStartTime() {
        return taskStartTime;
    }

    public void setTaskStartTime(Date taskStartTime) {
        this.taskStartTime = taskStartTime;
    }

    public Date getTaskStopTime() {
        return taskStopTime;
    }

    public void setTaskStopTime(Date taskStopTime) {
        this.taskStopTime = taskStopTime;
    }

    public String getErrorInfo() {
        return errorInfo;
    }

    public void setErrorInfo(String errorInfo) {
        this.errorInfo = errorInfo;
    }

    public String getCreatorUserName() {
        return creatorUserName;
    }

    public void setCreatorUserName(String creatorUserName) {
        this.creatorUserName = creatorUserName;
    }

    public Integer getAuditStatus() {
        return auditStatus;
    }

    public void setAuditStatus(Integer auditStatus) {
        this.auditStatus = auditStatus;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getAuditUserName() {
        return auditUserName;
    }

    public void setAuditUserName(String auditUserName) {
        this.auditUserName = auditUserName;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getModifyUserName() {
        return modifyUserName;
    }

    public void setModifyUserName(String modifyUserName) {
        this.modifyUserName = modifyUserName;
    }

    public Integer getIsCql() {
        return isCql;
    }

    public void setIsCql(Integer isCql) {
        this.isCql = isCql;
    }

    public String getTaskCql() {
        return taskCql;
    }

    public void setTaskCql(String taskCql) {
        this.taskCql = taskCql;
    }

    public Integer getEngineType() {
        return engineType;
    }

    public void setEngineType(Integer engineType) {
        this.engineType = engineType;
    }

    public String getEngineTypeShow() {
        return engineTypeShow;
    }

    public void setEngineTypeShow(String engineTypeShow) {
        this.engineTypeShow = engineTypeShow;
    }

    public String getArchiveId() {
        return archiveId;
    }

    public void setArchiveId(String archiveId) {
        this.archiveId = archiveId;
    }

    public String getArchiveVersionId() {
        return archiveVersionId;
    }

    public void setArchiveVersionId(String archiveVersionId) {
        this.archiveVersionId = archiveVersionId;
    }

    public String getTaskCqlId() {
        return taskCqlId;
    }

    public void setTaskCqlId(String taskCqlId) {
        this.taskCqlId = taskCqlId;
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

    public String getParallelism() {
        return parallelism;
    }

    public void setParallelism(String parallelism) {
        this.parallelism = parallelism;
    }
}
