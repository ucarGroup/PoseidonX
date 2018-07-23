package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;
import java.util.List;

/**
 * Description: jstorm exception 信息显示的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class JstormTopologyExceptionDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    public JstormTopologyExceptionDTO() {
    }

    private List<String> rootException;

    private List<String> workerExceptions;

    public List<String> getRootException() {
        return rootException;
    }

    public void setRootException(List<String> rootException) {
        this.rootException = rootException;
    }

    public List<String> getWorkerExceptions() {
        return workerExceptions;
    }

    public void setWorkerExceptions(List<String> workerExceptions) {
        this.workerExceptions = workerExceptions;
    }
}
