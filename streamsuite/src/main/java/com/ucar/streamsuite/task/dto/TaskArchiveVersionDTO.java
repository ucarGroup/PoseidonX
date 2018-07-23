package com.ucar.streamsuite.task.dto;

import java.io.Serializable;
import java.util.Date;

/**
 * Description:
 * Created on 2018/2/7 下午3:14
 *
 */
public class TaskArchiveVersionDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    private Integer id;
    private String taskArchiveVersionUrl;
    private String createUser;
    private String taskArchiveVersionRemark;
    private Date createTime;
    private Date updateTime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }


    public String getTaskArchiveVersionUrl() {
        return taskArchiveVersionUrl;
    }

    public void setTaskArchiveVersionUrl(String taskArchiveVersionUrl) {
        this.taskArchiveVersionUrl = taskArchiveVersionUrl;
    }

    public String getTaskArchiveVersionRemark() {
        return taskArchiveVersionRemark;
    }

    public void setTaskArchiveVersionRemark(String taskArchiveVersionRemark) {
        this.taskArchiveVersionRemark = taskArchiveVersionRemark;
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
}
