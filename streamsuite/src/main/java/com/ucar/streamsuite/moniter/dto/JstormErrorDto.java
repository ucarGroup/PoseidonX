
package com.ucar.streamsuite.moniter.dto;

import com.alibaba.jstorm.utils.TimeUtils;

public class JstormErrorDto {
    private int errorTime;
    private String error;
    private int errorLapsedSecs;

    public JstormErrorDto(int errorTime, String error) {
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
