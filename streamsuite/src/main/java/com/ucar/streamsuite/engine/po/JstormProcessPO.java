package com.ucar.streamsuite.engine.po;

import java.io.Serializable;
import java.util.Date;


/**
 * Description: 引擎jstorm执行的过程实例信息
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class JstormProcessPO implements Serializable {

    private static final long serialVersionUID = -5729044331250315761L;

    /**
     * id（自增id）
     */
    private Integer id;

    /**
     *  任务ID
     */
    private Integer taskId;

    /**
     *  拓扑ID
     */
    private String topId;

    /**
     * 任务开始时间
     */
    private Date startTime;

    /**
     * yarn环境上产生的APPID
     */
    private String yarnAppId;

    /**
     * 拓扑提交的日志信息
     */
    private String topLogMessage;

    /**
     * yarn环境元数据(json格式存储)
     */
    private String yarnAppMetadata;

    /**
     * 任务配置信息(json格式存储)
     */
    private String taskConfig;

    /**
     * 任务详情信息(json格式存储)
     */
    private String taskDetail;

    /**
     * 任务的提交类型
     */
    private Integer submitType;

    /**
     * 任务的提交结果
     */
    private Integer submitResult;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public String getTopId() {
        return topId;
    }

    public void setTopId(String topId) {
        this.topId = topId;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public String getYarnAppId() {
        return yarnAppId;
    }

    public void setYarnAppId(String yarnAppId) {
        this.yarnAppId = yarnAppId;
    }

    public String getTopLogMessage() {
        return topLogMessage;
    }

    public void setTopLogMessage(String topLogMessage) {
        this.topLogMessage = topLogMessage;
    }

    public String getTaskConfig() {
        return taskConfig;
    }

    public void setTaskConfig(String taskConfig) {
        this.taskConfig = taskConfig;
    }

    public Integer getSubmitType() {
        return submitType;
    }

    public void setSubmitType(Integer submitType) {
        this.submitType = submitType;
    }

    public String getYarnAppMetadata() {
        return yarnAppMetadata;
    }

    public void setYarnAppMetadata(String yarnAppMetadata) {
        this.yarnAppMetadata = yarnAppMetadata;
    }

    public Integer getSubmitResult() {
        return submitResult;
    }

    public void setSubmitResult(Integer submitResult) {
        this.submitResult = submitResult;
    }

    public String getTaskDetail() {
        return taskDetail;
    }

    public void setTaskDetail(String taskDetail) {
        this.taskDetail = taskDetail;
    }
}
