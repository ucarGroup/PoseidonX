package com.ucar.streamsuite.task.po;

import com.ucar.streamsuite.common.po.BaseTimeLineObject;

import java.io.Serializable;

/**
 * Description:
 * Created on 2018/2/7 下午2:52
 *
 */
public class TaskArchivePO extends BaseTimeLineObject implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    private Integer id;
    private String taskArchiveName;
    private String taskArchiveRemark;
    private Integer status;
    private Integer userGroupId;
    private String createUser;

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

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public void setTaskArchiveRemark(String taskArchiveRemark) {
        this.taskArchiveRemark = taskArchiveRemark;
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


}
