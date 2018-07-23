package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;

public class JstormClusterSupervisoDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250316880L;

    private String host;
    private String uptime;
    private String portsList;
    private String containerId;
    private String containerStats;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    public String getPortsList() {
        return portsList;
    }

    public void setPortsList(String portsList) {
        this.portsList = portsList;
    }

    public String getContainerId() {
        return containerId;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public String getContainerStats() {
        return containerStats;
    }

    public void setContainerStats(String containerStats) {
        this.containerStats = containerStats;
    }
}
