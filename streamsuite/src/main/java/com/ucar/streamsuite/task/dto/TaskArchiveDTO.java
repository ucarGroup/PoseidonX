package com.ucar.streamsuite.task.dto;

import java.io.Serializable;
import java.util.Date;

/**
 * Description:
 * Created on 2018/2/7 下午3:03
 *
 */
public class TaskArchiveDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    private Integer id;
    private String taskArchiveName;
    private String taskArchiveRemark;
    private Integer taskArchiveCount;
    private Integer status;
    private Integer userGroupId;
    private String createUser;
    private Date createTime;
    private Date updateTime;

    private String userGroupName;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTaskArchiveName() {
        return taskArchiveName;
    }

    public void setTaskArchiveName(String taskArchiveName) {
        this.taskArchiveName = taskArchiveName;
    }

    public String getTaskArchiveRemark() {
        return taskArchiveRemark;
    }

    public void setTaskArchiveRemark(String taskArchiveRemark) {
        this.taskArchiveRemark = taskArchiveRemark;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getUserGroupId() {
        return userGroupId;
    }

    public void setUserGroupId(Integer userGroupId) {
        this.userGroupId = userGroupId;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getUserGroupName() {
        return userGroupName;
    }

    public void setUserGroupName(String userGroupName) {
        this.userGroupName = userGroupName;
    }

    public Integer getTaskArchiveCount() {
        return taskArchiveCount;
    }

    public void setTaskArchiveCount(Integer taskArchiveCount) {
        this.taskArchiveCount = taskArchiveCount;
    }
}
