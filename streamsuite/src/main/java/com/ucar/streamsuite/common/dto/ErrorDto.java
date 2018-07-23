
package com.ucar.streamsuite.common.dto;

import com.alibaba.jstorm.utils.TimeUtils;

import java.io.Serializable;

public class ErrorDto implements Serializable {

    private static final long serialVersionUID = -5729144331250315760L;

    private int errorTime;
    private String error;
    private int errorLapsedSecs;

    public ErrorDto(int errorTime, String error) {
        this.errorTime = errorTime;
        this.error = error;
        this.errorLapsedSecs = TimeUtils.current_time_secs() - errorTime;
    }

    public int getErrorTime() {
        return errorTime;
    }

    public void setErrorTime(int errorTime) {
        this.errorTime = errorTime;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public int getErrorLapsedSecs() {
        return errorLapsedSecs;
    }

    public void setErrorLapsedSecs(int errorLapsedSecs) {
        this.errorLapsedSecs = errorLapsedSecs;
    }
}
