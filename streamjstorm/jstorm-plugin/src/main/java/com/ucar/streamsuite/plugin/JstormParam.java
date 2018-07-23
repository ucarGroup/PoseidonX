package com.ucar.streamsuite.plugin;

public class JstormParam {

    String zkroot;
    String zkAddress;
    String spoutworkers;
    String blotworkers;
    String workerjvmparam;
    String topworkers;
    String workermem;
    String supervisorlist;

    public String getSupervisorlist() {
        return supervisorlist;
    }

    public void setSupervisorlist(String supervisorlist) {
        this.supervisorlist = supervisorlist;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public String getWorkermem() {
        return workermem;
    }

    public void setWorkermem(String workermem) {
        this.workermem = workermem;
    }

    public String getTopworkers() {
        return topworkers;
    }

    public void setTopworkers(String topworkers) {
        this.topworkers = topworkers;
    }

    public String getZkroot() {
        return zkroot;
    }

    public void setZkroot(String zkroot) {
        this.zkroot = zkroot;
    }

    public String getSpoutworkers() {
        return spoutworkers;
    }

    public void setSpoutworkers(String spoutworkers) {
        this.spoutworkers = spoutworkers;
    }

    public String getBlotworkers() {
        return blotworkers;
    }

    public void setBlotworkers(String blotworkers) {
        this.blotworkers = blotworkers;
    }

    public String getWorkerjvmparam() {
        return workerjvmparam;
    }

    public void setWorkerjvmparam(String workerjvmparam) {
        this.workerjvmparam = workerjvmparam;
    }
}
