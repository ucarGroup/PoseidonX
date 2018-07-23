package com.ucar.streamsuite.yarn.appmaster;

/**
 * Description: jstorm yarn任务提交DTO
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class JstormYarnAppSubmitDTO {

    private static final long serialVersionUID = -5729044331250316960L;

    /**
     * nimbus个数
     */
    private Integer nimbusNum;

    /**
     * supervisor个数
     */
    private Integer supervisorNum;

    /**
     * nimbus内存数单位 MB
     */
    private Integer nimbusMemery;

    /**
     * supervisor内存数 单位 MB
     */
    private Integer supervisorMemery;

    /**
     * jstorm_版本包的位置
     */
    private String jstormJarPath;

    /**
     * am 版本包的位置
     */
    private String yarnAmJarPath;


    /**
     * jstorm zk的主机 多个之间用逗号分开 (ip1,ip2)
     */
    private String jstormZkHost;

    /**
     * jstorm zk端口
     */
    private Integer jstormZkPort;

    /**
     * jstorm zkRoot  = (前缀 + 任务名)
     */
    private String jstormZkRoot;

    /**
     * yarn环境上产生的APPID
     */
    private String yarnAppId;

    /**
     * 提交到yarn环境的系统用户
     */
    private String yarnSubmitter;

    /**
     * supervisor port数
     */
    private Integer supervisorportNum;

    /**
     * 提交到yarn环境jstorm集群名称
     */
    private String clusterName;

    public Integer getNimbusNum() {
        return nimbusNum;
    }

    public void setNimbusNum(Integer nimbusNum) {
        this.nimbusNum = nimbusNum;
    }

    public Integer getSupervisorNum() {
        return supervisorNum;
    }

    public void setSupervisorNum(Integer supervisorNum) {
        this.supervisorNum = supervisorNum;
    }

    public Integer getNimbusMemery() {
        return nimbusMemery;
    }

    public void setNimbusMemery(Integer nimbusMemery) {
        this.nimbusMemery = nimbusMemery;
    }

    public Integer getSupervisorMemery() {
        return supervisorMemery;
    }

    public void setSupervisorMemery(Integer supervisorMemery) {
        this.supervisorMemery = supervisorMemery;
    }

    public String getJstormJarPath() {
        return jstormJarPath;
    }

    public void setJstormJarPath(String jstormJarPath) {
        this.jstormJarPath = jstormJarPath;
    }

    public String getYarnAmJarPath() {
        return yarnAmJarPath;
    }

    public void setYarnAmJarPath(String yarnAmJarPath) {
        this.yarnAmJarPath = yarnAmJarPath;
    }

    public String getJstormZkHost() {
        return jstormZkHost;
    }

    public void setJstormZkHost(String jstormZkHost) {
        this.jstormZkHost = jstormZkHost;
    }

    public Integer getJstormZkPort() {
        return jstormZkPort;
    }

    public void setJstormZkPort(Integer jstormZkPort) {
        this.jstormZkPort = jstormZkPort;
    }

    public String getJstormZkRoot() {
        return jstormZkRoot;
    }

    public void setJstormZkRoot(String jstormZkRoot) {
        this.jstormZkRoot = jstormZkRoot;
    }

    public String getYarnAppId() {
        return yarnAppId;
    }

    public void setYarnAppId(String yarnAppId) {
        this.yarnAppId = yarnAppId;
    }

    public String getYarnSubmitter() {
        return yarnSubmitter;
    }

    public void setYarnSubmitter(String yarnSubmitter) {
        this.yarnSubmitter = yarnSubmitter;
    }

    public Integer getSupervisorportNum() {
        return supervisorportNum;
    }

    public void setSupervisorportNum(Integer supervisorportNum) {
        this.supervisorportNum = supervisorportNum;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
}
