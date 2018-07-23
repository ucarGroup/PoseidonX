package com.ucar.streamsuite.engine.dto;

import com.google.common.collect.Lists;
import com.ucar.streamsuite.common.util.DateUtil;

import java.io.Serializable;
import java.util.List;

/**
 * Description: jstorm 拓扑信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class JstormTopologyDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    private String topId;
    private String name;
    private String status;
    private int tasksTotal;
    private int workersTotal;
    private String errorInfo;
    private String uptime;

    private List<JstormCompenentDTO> compenentInfos = Lists.newArrayList();

    public JstormTopologyDTO() {
    }

    public JstormTopologyDTO(String topId, String name, String status, int uptimeSeconds,
                          int tasksTotal, int workersTotal, String errorInfo) {
        this.topId = topId;
        this.name = name;
        this.status = status;
        this.tasksTotal = tasksTotal;
        this.workersTotal = workersTotal;
        this.errorInfo = errorInfo;
        this.uptime = DateUtil.prettyUptime(uptimeSeconds);
    }

    public List<JstormCompenentDTO> getCompenentInfos() {
        return compenentInfos;
    }

    public void setCompenentInfos(List<JstormCompenentDTO> compenentInfos) {
        this.compenentInfos = compenentInfos;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    public String getTopId() {
        return topId;
    }

    public void setTopId(String topId) {
        this.topId = topId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getTasksTotal() {
        return tasksTotal;
    }

    public void setTasksTotal(int tasksTotal) {
        this.tasksTotal = tasksTotal;
    }

    public int getWorkersTotal() {
        return workersTotal;
    }

    public void setWorkersTotal(int workersTotal) {
        this.workersTotal = workersTotal;
    }

    public String getErrorInfo() {
        return errorInfo;
    }

    public void setErrorInfo(String errorInfo) {
        this.errorInfo = errorInfo;
    }
}
