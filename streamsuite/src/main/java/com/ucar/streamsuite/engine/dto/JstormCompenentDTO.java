package com.ucar.streamsuite.engine.dto;

import backtype.storm.generated.ErrorInfo;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.ucar.streamsuite.common.dto.ErrorDto;
import com.ucar.streamsuite.common.dto.HostDto;
import org.assertj.core.util.Sets;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Description: jstorm 拓扑信息的组件的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class JstormCompenentDTO implements Serializable {

    private static final long serialVersionUID = -5729044331251325860L;

    private String name;
    private String type;

    private Integer tasksActiveCount;
    private Integer tasksStartingCount;
    private Integer tasksTotal;
    private String errorMessage;
    private Set<HostDto> workers = Sets.newHashSet();


    public Set<HostDto> getWorkers() {
        return workers;
    }

    public void setWorkers(Set<HostDto> workers) {
        this.workers = workers;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Integer getTasksActiveCount() {
        return tasksActiveCount;
    }

    public void setTasksActiveCount(Integer tasksActiveCount) {
        this.tasksActiveCount = tasksActiveCount;
    }

    public Integer getTasksStartingCount() {
        return tasksStartingCount;
    }

    public void setTasksStartingCount(Integer tasksStartingCount) {
        this.tasksStartingCount = tasksStartingCount;
    }

    public Integer getTasksTotal() {
        return tasksTotal;
    }

    public void setTasksTotal(Integer tasksTotal) {
        this.tasksTotal = tasksTotal;
    }

    public JstormCompenentDTO() {
    }

    public JstormCompenentDTO(String name, String type, List<ErrorInfo> errors) {
        this.name = name;
        this.type = type;
        this.setErrors(errors);
    }

    private transient List<ErrorDto> errors;
    private transient String taskInfo;

    public void setErrors(List<ErrorInfo> errors) {
        if (errors == null){
            this.errors = null;
            return;
        }
        this.errors = new ArrayList<ErrorDto>();
        for (ErrorInfo info : errors){
            ErrorDto err = new ErrorDto(info.get_errorTimeSecs(), info.get_error());
            this.errors.add(err);
        }

        Set<String> error =Sets.newHashSet();
        for(ErrorDto  ee: this.errors){
            error.add(ee.getError());
        }
        if(error.size()>=0){
            this.errorMessage = JSONObject.toJSONString(error);
        }
    }

    public String getTaskInfo() {
        return taskInfo;
    }

    public void setTaskInfo(String taskInfo) {
        this.taskInfo = taskInfo;
    }

    public void setWorkersByTaskDtos(List<JstormCompenentTaskDTO> jstormCompenentTaskDTOs) {
        if(jstormCompenentTaskDTOs==null){
            return;
        }
        this.tasksTotal = jstormCompenentTaskDTOs.size();
        for(JstormCompenentTaskDTO taskInfo:jstormCompenentTaskDTOs){
            workers.add(new HostDto(taskInfo.getHost(),taskInfo.getPort()));
        }
    }
}
