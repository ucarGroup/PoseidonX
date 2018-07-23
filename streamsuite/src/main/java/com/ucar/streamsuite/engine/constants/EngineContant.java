package com.ucar.streamsuite.engine.constants;

import com.alibaba.jstorm.metric.AsmWindow;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.ConfigKeyEnum;

/**
 * Description: jstorm相关的常量类
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class EngineContant {

    // 拓扑的状态
    public static final String TOP_STATUS_ACTIVE = "active";
    public static final String TOP_STATUS_INACTIVE = "inactive";
    public static final String TOP_STATUS_STARTING = "starting";
    public static final String TOP_STATUS_KILLED = "killed";
    public static final String TOP_STATUS_REBALANCING = "rebalancing";
    // worker的状态
    public static final String WORKER_STATUS_ACTIVE = "active";
    public static final String WORKER_STATUS_INACTIVE = "inactive";
    public static final String WORKER_STATUS_STARTING = "starting";

    // 集群的状态
    public static final String ACTIVE = "active";
    public static final String INACTIVE = "inactive";

    //默认port数
    public static final Integer SUPERVISOR_DEFAULT_PORT= 5;

    //一个容器的SUPERVISOR 自身占用的内存数
    public static final Integer SUPERVISOR_OWNER_MEM= 1024;
    //一个容器的nimbus 自身占用的内存数
    public static final Integer NIMBUS_OWNER_MEM= 1024;
    //JOB_MANGER默认占内存数
    public static final Long JOB_MANGER_DEFAULT_MEM= 2048L;

    // AM的优先级
    public static final Integer AM_DEFAULT_PRIORITY = 0;
    // AM的队列
    public static final String AM_DEFAULT_QUEUE = "default";
    // AM申请的内存大小
    public static final Integer AM_DEFAULT_MEMORY = 500;
    // AM申请的核数
    public static final Integer AM_DEFAULT_VCORES = 1;
    // AM的启动类
    public static final String AM_CLASS = "com.ucar.streamsuite.yarn.appmaster.JstormMaster";

    // yarn task提交的锁前缀
    public static final String TASK_SUBMIT_LOCK_PRE  = "/streamSuite/taskSubmitLock/";
    // yarn task停止的锁前缀
    public static final String TASK_STOP_LOCK_PRE  = "/streamSuite/taskStopLock/";

    // yarn task提交的时间线数据存储
    public static final String TASK_SUBMIT_TIMELINE_PRE  = "/streamSuite/taskSubmitTimeLine/";
    // yarn app 在jstorm集群上的 zk root 前缀
    public static final String YARN_APP_PREFIX =  "/streamSuite/yarn/jstormcluster/";

    // TIMELINE 状态
    public static final String TIMELINE_ITEM_OK  = "green";
    public static final String TIMELINE_ITEM_FAILD  = "red";
    public static final String TIMELINE_OK  = "green";
    public static final String TIMELINE_FAILD  = "red";

    public static final String COMPONENT_ACKER_NAME  = "__acker";
    public static final String COMPONENT_TOPOLOGY_MASTER_NAME  = "__topology_master";

    public static final Integer SSH_DEFAULT_PORT  = 22;

    //这个用于表示CQL提交jstorm任务的时候,用来用conf的map里面选择rtcp的jar路径的key名字
    public static final String RTCP_CQL_JAR_PATH_NAME = "RTCP_CQL_JAR_PATH";

    //是否 进行提交, false 为不提交
    public static final String RTCP_CQL_IS_SUBMIT = "isSubmit";

    public static String FLINK_HOME = ConfigProperty.getConfigValue(ConfigKeyEnum.FLINK_HOME);

    public static String JSTORM_HOME = ConfigProperty.getConfigValue(ConfigKeyEnum.JSTORM_HOME);

    public static String JSTORM_CLUSTER_RECOVERY_FLAG = ConfigProperty.getConfigValue(ConfigKeyEnum.JSTORM_CLUSTER_RECOVERY_FLAG);

    public static String HADOOP_USER_PASSWORD = "";

    //执行jstorm 检查点的时间间隔
    public static final Integer JSTORM_CHECKPOINT_CYCLE = 60;
    //执行flink 检查点的时间间隔
    public static final Integer FLINK_CHECKPOINT_CYCLE = 60;

    public final static String LOG_spliter = "||||";

    //flink cql 类名
    public static final String FLINK_SQL_MAIN_CLASS = "com.ucar.flinksql.FlinkSqlTaskMain";
}
