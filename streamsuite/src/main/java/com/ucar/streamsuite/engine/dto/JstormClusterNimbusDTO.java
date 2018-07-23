package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;

public class JstormClusterNimbusDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315880L;

    private String host;
    private String uptime;
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
