package com.ucar.streamsuite.engine.dto;


import backtype.storm.generated.ErrorInfo;
import com.ucar.streamsuite.common.dto.ErrorDto;
import com.ucar.streamsuite.common.util.DateUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: jstorm worker线程的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class JstormCompenentTaskDTO {

    private static final long serialVersionUID = -5729044331250317860L;

    private Integer id;
    private String component;
    private int uptimeSeconds;
    private String status;
    private String host;
    private Integer port;
    private String errorMessage;
    private List<ErrorDto> errors;

    public JstormCompenentTaskDTO() {
    }

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

        String errormessage="";
        for(ErrorDto  ee: this.errors){
            errormessage+= ee.getError()+",";
        }
        this.errorMessage = errormessage;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getComponent() {
        return component;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public void setUptimeSeconds(int uptimeSeconds) {
        this.uptimeSeconds = uptimeSeconds;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public int getUptimeSeconds() {
        return uptimeSeconds;
    }

}
