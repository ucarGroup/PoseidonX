package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;
import java.util.Date;
import java.util.List;


/**
 * Description: 引擎执行的过程DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class ProcessDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315771L;

    /**
     * rowId
     */
    private String rowId;

    /**
     * yarn环境上产生的APPID
     */
    private String yarnAppId;

    /**
     * 任务开始时间
     */
    private Date startTime;

    /**
     * 拓扑提交的日志信息
     */
    private List<String> logMessage;

    /**
     * 任务配置信息
     */
    private List<String> taskConfig;

    /**
     * 类型 恢复还是提交
     */
    private String type;

    /**
     * 提交结果
     */
    private Integer result;

    public String getYarnAppId() {
        return yarnAppId;
    }

    public void setYarnAppId(String yarnAppId) {
        this.yarnAppId = yarnAppId;
    }

    public Date getStartTime() {
        return startTime;
    }

    public List<String> getLogMessage() {
        return logMessage;
    }

    public void setLogMessage(List<String> logMessage) {
        this.logMessage = logMessage;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Integer getResult() {
        return result;
    }

    public void setResult(Integer result) {
        this.result = result;
    }

    public List<String> getTaskConfig() {
        return taskConfig;
    }

    public void setTaskConfig(List<String> taskConfig) {
        this.taskConfig = taskConfig;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }
}
