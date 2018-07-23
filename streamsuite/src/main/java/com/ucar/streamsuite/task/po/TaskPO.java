package com.ucar.streamsuite.task.po;

import com.ucar.streamsuite.common.po.BaseTimeLineObject;

import java.io.Serializable;
import java.util.Date;

/**
 * Description: 任务信息
 * Created on 2018/1/18 下午4:33
 *
 */
public class TaskPO extends BaseTimeLineObject implements Serializable {

    private static final long serialVersionUID = -5729044331250314760L;

    /**
     * 任务id（自增id）
     */
    private Integer id;

    /**
     * 任务名(英文，必须以字母开头，并且只能包含字母数字或下划线，且唯一)
     */
    private String taskName;

    /**
     * 任务类型（TaskTypeEnum）
     */
    private Integer taskType;

    /**
     * 任务状态（TaskStatusEnum）
     */
    private Integer taskStatus;

    /**
     * 审核状态（AuditStatusEnum）
     */
    private Integer auditStatus;

    /**
     * 审核时间
     */
    private Date auditTime;

    /**
     * 审核人
     */
    private String auditUserName;

    /**
     * 备注
     */
    private String remark;

    /**
     * 任务配置信息(json格式存储)
     */
    private String taskConfig;

    /**
     * 引擎类型（EngineTypeEnum）
     */
    private Integer engineType;

    /**
     * 任务执行过程id
     */
    private Integer processId;

    /**
     * 任务开始时间（任务最近一次成功开始的时间）
     */
    private Date taskStartTime;

    /**
     * 任务结束时间（手动停止的时间）
     */
    private Date taskStopTime;

    /**
     * 用户组ID。（预留，以后用于权限控制）
     */
    private Integer userGroupId;

    /**
     * 状态（CommonStatusEnum）
     */
    private Integer deleteStatus;

    /**
     * 任务创建人(任务信息的创建人)
     */
    private String creatorUserName;

    /**
     * 任务修改人(最近一次编辑任务信息的修改人)
     */
    private String modifyUserName;

    /**
     * 任务执行过程中最近一次的异常信息
     */
    private String errorInfo;

    /**
     * 项目ID
     */
    private Integer archiveId;

    /**
     * 项目 包的ID
     */
    private Integer archiveVersionId;

    /**
     * 是否是CQL任务（CQL任务需要存 taskCQL 和 cqlID）
     */
    private Integer isCql;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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

    public String getAuditUserName() {
        return auditUserName;
    }

    public void setAuditUserName(String auditUserName) {
        this.auditUserName = auditUserName;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getTaskConfig() {
        return taskConfig;
    }

    public void setTaskConfig(String taskConfig) {
        this.taskConfig = taskConfig;
    }

    public Integer getEngineType() {
        return engineType;
    }

    public void setEngineType(Integer engineType) {
        this.engineType = engineType;
    }

    public Integer getProcessId() {
        return processId;
    }

    public void setProcessId(Integer processId) {
        this.processId = processId;
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

    public Integer getUserGroupId() {
        return userGroupId;
    }

    public void setUserGroupId(Integer userGroupId) {
        this.userGroupId = userGroupId;
    }

    public Integer getDeleteStatus() {
        return deleteStatus;
    }

    public void setDeleteStatus(Integer deleteStatus) {
        this.deleteStatus = deleteStatus;
    }

    public String getCreatorUserName() {
        return creatorUserName;
    }

    public void setCreatorUserName(String creatorUserName) {
        this.creatorUserName = creatorUserName;
    }

    public String getModifyUserName() {
        return modifyUserName;
    }

    public void setModifyUserName(String modifyUserName) {
        this.modifyUserName = modifyUserName;
    }

    public String getErrorInfo() {
        return errorInfo;
    }

    public void setErrorInfo(String errorInfo) {
        this.errorInfo = errorInfo;
    }

    public Integer getAuditStatus() {
        return auditStatus;
    }

    public void setAuditStatus(Integer auditStatus) {
        this.auditStatus = auditStatus;
    }

    public Integer getArchiveId() {
        return archiveId;
    }

    public void setArchiveId(Integer archiveId) {
        this.archiveId = archiveId;
    }

    public Integer getIsCql() {
        return isCql;
    }

    public void setIsCql(Integer isCql) {
        this.isCql = isCql;
    }

    public Integer getArchiveVersionId() {
        return archiveVersionId;
    }

    public void setArchiveVersionId(Integer archiveVersionId) {
        this.archiveVersionId = archiveVersionId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskPO taskPO = (TaskPO) o;

        return id != null ? id.equals(taskPO.id) : taskPO.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
