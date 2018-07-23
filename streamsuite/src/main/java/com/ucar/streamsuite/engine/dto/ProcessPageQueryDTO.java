package com.ucar.streamsuite.engine.dto;

import com.ucar.streamsuite.common.dto.PageQueryDTO;

/**
 * Description: 引擎jstorm执行的过程DTO 分页查询
 * Created on 2018/1/18 下午4:33
 *
 */
public class ProcessPageQueryDTO extends PageQueryDTO {

    private static final long serialVersionUID = -5729044331250315772L;

    /**
     * 任务id
     */
    private Integer taskId;

    public ProcessPageQueryDTO(){}

    public ProcessPageQueryDTO(Integer pageNum, Integer pageSize) {
        super(pageNum,pageSize);
    }

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }
}
