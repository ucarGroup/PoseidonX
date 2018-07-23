package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;

/**
 * Description: flink taskManager 信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class FlinkTaskManagerDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    public FlinkTaskManagerDTO() {
    }

    private String id;
    private String path;
    private Long timeSinceLastHeartbeat;
    private Integer dataPort;
    private Integer slotsNumber;
    private Integer freeSlots;
    private Integer cpuCores;
    private Long physicalMemory;
    private Long freeMemory;
    private Long managedMemory;

    private transient String lastHeartbeat;
    private transient String physicalMemoryShow;
    private transient String freeMemoryShow;
    private transient String managedMemoryShow;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getDataPort() {
        return dataPort;
    }

    public void setDataPort(Integer dataPort) {
        this.dataPort = dataPort;
    }

    public Integer getSlotsNumber() {
        return slotsNumber;
    }

    public void setSlotsNumber(Integer slotsNumber) {
        this.slotsNumber = slotsNumber;
    }

    public Integer getFreeSlots() {
        return freeSlots;
    }

    public void setFreeSlots(Integer freeSlots) {
        this.freeSlots = freeSlots;
    }

    public Integer getCpuCores() {
        return cpuCores;
    }

    public void setCpuCores(Integer cpuCores) {
        this.cpuCores = cpuCores;
    }

    public Long getPhysicalMemory() {
        return physicalMemory;
    }

    public void setPhysicalMemory(Long physicalMemory) {
        this.physicalMemory = physicalMemory;
    }

    public Long getFreeMemory() {
        return freeMemory;
    }

    public void setFreeMemory(Long freeMemory) {
        this.freeMemory = freeMemory;
    }

    public Long getManagedMemory() {
        return managedMemory;
    }

    public void setManagedMemory(Long managedMemory) {
        this.managedMemory = managedMemory;
    }

    public Long getTimeSinceLastHeartbeat() {
        return timeSinceLastHeartbeat;
    }

    public void setTimeSinceLastHeartbeat(Long timeSinceLastHeartbeat) {
        this.timeSinceLastHeartbeat = timeSinceLastHeartbeat;
    }

    public String getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(String lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public String getPhysicalMemoryShow() {
        return physicalMemoryShow;
    }

    public void setPhysicalMemoryShow(String physicalMemoryShow) {
        this.physicalMemoryShow = physicalMemoryShow;
    }

    public String getFreeMemoryShow() {
        return freeMemoryShow;
    }

    public void setFreeMemoryShow(String freeMemoryShow) {
        this.freeMemoryShow = freeMemoryShow;
    }

    public String getManagedMemoryShow() {
        return managedMemoryShow;
    }

    public void setManagedMemoryShow(String managedMemoryShow) {
        this.managedMemoryShow = managedMemoryShow;
    }
}
