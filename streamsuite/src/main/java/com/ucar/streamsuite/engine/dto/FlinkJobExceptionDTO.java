package com.ucar.streamsuite.engine.dto;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.List;

/**
 * Description: flink job exception 信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class FlinkJobExceptionDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;


    public FlinkJobExceptionDTO() {
    }

    private String rootException;
    private transient List<String> rootExceptionShow;

    private List<FlinkJobExceptionDetailDTO> allExceptions;

    @JSONField(name="all-exceptions")
    public List<FlinkJobExceptionDetailDTO> getAllExceptions() {
        return allExceptions;
    }

    @JSONField(name="all-exceptions")
    public void setAllExceptions(List<FlinkJobExceptionDetailDTO> allExceptions) {
        this.allExceptions = allExceptions;
    }

    @JSONField(name="root-exception")
    public String getRootException() {
        return rootException;
    }

    @JSONField(name="root-exception")
    public void setRootException(String rootException) {
        this.rootException = rootException;
    }

    public List<String> getRootExceptionShow() {
        return rootExceptionShow;
    }

    public void setRootExceptionShow(List<String> rootExceptionShow) {
        this.rootExceptionShow = rootExceptionShow;
    }
}
