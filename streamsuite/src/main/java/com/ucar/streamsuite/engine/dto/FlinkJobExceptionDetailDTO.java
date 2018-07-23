package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;
import java.util.List;

/**
 * Description: flink job exception 信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class FlinkJobExceptionDetailDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;


    public FlinkJobExceptionDetailDTO() {
    }

    private transient Integer id;
    private transient List<String> exceptionShow;

    private String exception;
    private String task;
    private String location;


    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public String getTask() {
        return task;
    }

    public void setTask(String task) {
        this.task = task;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public List<String> getExceptionShow() {
        return exceptionShow;
    }

    public void setExceptionShow(List<String> exceptionShow) {
        this.exceptionShow = exceptionShow;
    }
}
