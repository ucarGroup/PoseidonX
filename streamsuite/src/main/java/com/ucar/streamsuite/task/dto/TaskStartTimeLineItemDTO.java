package com.ucar.streamsuite.task.dto;

/**
 * Description: 任务开始过程的时间线的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class TaskStartTimeLineItemDTO {

    private static final long serialVersionUID = -5729044335250315890L;

    private String content;

    private String time;

    private String status;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
