package com.ucar.streamsuite.engine.dto;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

public class JstormClusterDTO implements Serializable {

    private static final long serialVersionUID = -5729054331250315880L;

    private String appId;
    private String appAmHost;
    private String appAmPort;
    private String appStartTime;
    private String appState;

    private String clusterName;
    private String clusterZkRoot;
    private String clusterZkPort;
    private String clusterZkHost;
    private String clusterSlotsUsed;
    private String clusterSlotsTotal;
    private String clusterSlots;

    private String clusterSupervisorSize;
    private String topId;

    private List<JstormClusterNimbusDTO> nimbusInfos = Lists.newArrayList();
    private List<JstormClusterSupervisoDTO> supervisorInfos = Lists.newArrayList();

    public String getClusterSlots() {
        return clusterSlots;
    }

    public void setClusterSlots(String clusterSlots) {
        this.clusterSlots = clusterSlots;
    }

    public void setClusterSupervisorSize(String clusterSupervisorSize) {
        this.clusterSupervisorSize = clusterSupervisorSize;
    }

    public String getClusterSupervisorSize() {
        return clusterSupervisorSize;
    }

    public void addNimbusInfo(JstormClusterNimbusDTO nimbusInfo) {
        nimbusInfos.add(nimbusInfo);
    }

    public void addSupervisorInfo(JstormClusterSupervisoDTO supervisorInfo) {
        supervisorInfos.add(supervisorInfo);
        clusterSupervisorSize = String.valueOf(supervisorInfos.size());
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppAmHost() {
        return appAmHost;
    }

    public void setAppAmHost(String appAmHost) {
        this.appAmHost = appAmHost;
    }

    public String getAppAmPort() {
        return appAmPort;
    }

    public void setAppAmPort(String appAmPort) {
        this.appAmPort = appAmPort;
    }

    public String getAppStartTime() {
        return appStartTime;
    }

    public void setAppStartTime(String appStartTime) {
        this.appStartTime = appStartTime;
    }

    public String getAppState() {
        return appState;
    }

    public void setAppState(String appState) {
        this.appState = appState;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterZkRoot() {
        return clusterZkRoot;
    }

    public void setClusterZkRoot(String clusterZkRoot) {
        this.clusterZkRoot = clusterZkRoot;
    }

    public String getClusterZkPort() {
        return clusterZkPort;
    }

    public void setClusterZkPort(String clusterZkPort) {
        this.clusterZkPort = clusterZkPort;
    }

    public String getClusterZkHost() {
        return clusterZkHost;
    }

    public void setClusterZkHost(String clusterZkHost) {
        this.clusterZkHost = clusterZkHost;
    }

    public String getClusterSlotsUsed() {
        return clusterSlotsUsed;
    }

    public void setClusterSlotsUsed(String clusterSlotsUsed) {
        this.clusterSlotsUsed = clusterSlotsUsed;
    }

    public String getClusterSlotsTotal() {
        return clusterSlotsTotal;
    }

    public void setClusterSlotsTotal(String clusterSlotsTotal) {
        this.clusterSlotsTotal = clusterSlotsTotal;
    }

    public String getTopId() {
        return topId;
    }

    public void setTopId(String topId) {
        this.topId = topId;
    }

    public List<JstormClusterNimbusDTO> getNimbusInfos() {
        return nimbusInfos;
    }

    public void setNimbusInfos(List<JstormClusterNimbusDTO> nimbusInfos) {
        this.nimbusInfos = nimbusInfos;
    }

    public List<JstormClusterSupervisoDTO> getSupervisorInfos() {
        return supervisorInfos;
    }

    public void setSupervisorInfos(List<JstormClusterSupervisoDTO> supervisorInfos) {
        this.supervisorInfos = supervisorInfos;
    }
}
