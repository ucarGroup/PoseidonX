package com.ucar.streamsuite.task.dto;

import com.ucar.streamsuite.common.dto.PageQueryDTO;

import java.util.Set;

/**
 * Description: 任务的分页查询条件
 * Created on 2018/1/31 下午4:12
 *
 */
public class TaskPageQueryDTO extends PageQueryDTO {

    private static final long serialVersionUID = -5729044331250315761L;

    /**
     * 任务状态（TaskStatusEnum）
     */
    private Integer taskStatusCondition;

    /**
     * 审核状态（AuditStatusEnum）
     */
    private Integer auditStatusCondition;

    /**
     * 引擎类型（EngineTypeEnum）
     */
    private Integer engineTypeCondition;

    /**
     * 任务名
     */
    private String taskNameCondition;

    /**
     * 用户名(管理员可以通过用户名查，普通用户根据权限走)
     */
    private String creatorUserNameCondition;

    /**
     * 当前用户名 (普通用户根据权限走此条件部位空)
     */
    private String currentUserNameCondition;

    public TaskPageQueryDTO(){}

    public TaskPageQueryDTO(Integer pageNum, Integer pageSize) {
        super(pageNum,pageSize);
    }

    public Integer getTaskStatusCondition() {
        return taskStatusCondition;
    }

    public void setTaskStatusCondition(Integer taskStatusCondition) {
        this.taskStatusCondition = taskStatusCondition;
    }

    public Integer getAuditStatusCondition() {
        return auditStatusCondition;
    }

    public void setAuditStatusCondition(Integer auditStatusCondition) {
        this.auditStatusCondition = auditStatusCondition;
    }

    public String getTaskNameCondition() {
        return taskNameCondition;
    }

    public void setTaskNameCondition(String taskNameCondition) {
        this.taskNameCondition = taskNameCondition;
    }

    public String getCreatorUserNameCondition() {
        return creatorUserNameCondition;
    }

    public void setCreatorUserNameCondition(String creatorUserNameCondition) {
        this.creatorUserNameCondition = creatorUserNameCondition;
    }

    public String getCurrentUserNameCondition() {
        return currentUserNameCondition;
    }

    public void setCurrentUserNameCondition(String currentUserNameCondition) {
        this.currentUserNameCondition = currentUserNameCondition;
    }

    public Integer getEngineTypeCondition() {
        return engineTypeCondition;
    }

    public void setEngineTypeCondition(Integer engineTypeCondition) {
        this.engineTypeCondition = engineTypeCondition;
    }
}
