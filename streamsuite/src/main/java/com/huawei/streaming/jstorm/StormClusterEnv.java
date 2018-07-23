package com.huawei.streaming.jstorm;

/**
 * Created   on 2016/5/14.
 */
public enum StormClusterEnv {

    CUSTOM("custom"),
    MONITOR("monitor");

    private String clusterEnv;

    StormClusterEnv(String clusterEnv) {
        this.clusterEnv = clusterEnv;
    }

    public String toString() {
        return clusterEnv;
    }

}
