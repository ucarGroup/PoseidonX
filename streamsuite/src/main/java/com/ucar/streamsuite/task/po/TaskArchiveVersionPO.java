package com.ucar.streamsuite.task.po;

import com.ucar.streamsuite.common.po.BaseTimeLineObject;

import java.io.Serializable;

/**
 * Description:
 * Created on 2018/2/7 下午3:01
 *
 */
public class TaskArchiveVersionPO extends BaseTimeLineObject implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    private Integer id;
    private String taskArchiveVersionUrl;
    private Integer taskArchiveId;
    private String createUser;
    private String taskArchiveVersionRemark;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }


    public Integer getTaskArchiveId() {
        return taskArchiveId;
    }

    public void setTaskArchiveId(Integer taskArchiveId) {
        this.taskArchiveId = taskArchiveId;
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
}
