package com.ucar.streamsuite.yarn.appmaster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.AMRMClient;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * jstorm AM 的上下文
 */
public class JstormAMContext {

    //AM的所在机器名
    public static String appMasterHostname;
    //AM的端口号
    public static int appMasterHostPort;
    // Tracking url to which app master publishes info for clients to monitor
    public static String appMasterTrackingUrl = "";

    public static int maxMemory;
    //yarn集群目前最大可用虚拟cpu数
    public static int maxVcores;

    //应用申请的内存
    public static int applyMemoryForNimbusContainer;
    public static int applyMemoryForSupervisorContainer;
    //应用申请的cpu数
    public static int applyVcores;
    //当前是否运行,如果为true则运行,为false则表示停止运行
    public static boolean isRunning;

    public static String serviceUserName;
    //nimbus的数量
    public static Integer nimbusNum;
    //nimbus的内存
    public static Integer nimbusMemory;
    //supervisor的数量
    public static Integer supervisorNum;
    //supervisor的内存
    public static Integer supervisorMemory;
    //jstorm集群文件的路径
    public static String  jstormJarPath;
    //jstormAM文件的路径
    public static String  appMasterJarPath;
    //zk地址
    public static String  zkAddress;
    //zk端口
    public static String  zkPort;
    //zk路径前缀
    public static String  zkRoot;
    //jstorm集群名字,实际就是 zkRoot 的最后一个词
    public static String clusterName;
    //应用id
    public static String appId;
    //hadoop 配置
    public static Configuration conf;
    //用户名
    public static String userName;

    //token
    public static ByteBuffer allTokens;

    public static ContainerId containerForAMId;
    public static ApplicationAttemptId applicationAttemptId;
    public static ApplicationId applicationId;

    public static int nimbusPort;
    public static int logviewPort;

    //supervisor的个数
    public static int supervisorPortNum;

    public static BlockingQueue<AMRMClient.ContainerRequest> requestBlockingQueue;

    //当前已分配的nimbus container数量
    public static AtomicInteger currentNimbusNum;

    //当前已经分配的supervisor container 数量
    public static AtomicInteger currentSupervisorNum;

}
