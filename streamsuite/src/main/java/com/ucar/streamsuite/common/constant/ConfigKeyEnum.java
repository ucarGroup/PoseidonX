package com.ucar.streamsuite.common.constant;

/**
 * Description: 常用配置项的key ,和配置管理模块的配置名字相对应
 * Created on 2018/2/1 上午10:45
 *
 */
public enum ConfigKeyEnum {

    /**
     * 实时引擎用到的zk地址
     */
    ZK_HOST,

    /**
     * 实时引擎用到的zk端口
     */
    ZK_PORT,

    /**
     * 本地项目包上传路径
     */
    LOCAL_PROJECT_ITEM_DIR,

    /**
     * CQL引擎的项目文件名称
     */
    CQL_ENGINE_PROJECT_NAME,

    /**
     * 本地的JSTORM路径
     */
    JSTORM_HOME,

    /**
     * NIMBUS 内存，不配置默认为 1024m
     */
    JSTORM_NIMBUS_MEM,

    /**
     * 任务日志前缀
     */
    JSTORM_TASK_LOG_PREFIX,

    /**
     * JSTORM 集群死亡时是否强制恢复(默认为0，1为打开)
     */
    JSTORM_CLUSTER_RECOVERY_FLAG,

    /**
     * JSTORM 自动恢复的重试次数(默认为3次)
     */
    JSTORM_AUTO_RECOVERY_RETRY,

    /**
     * FLINK 自动恢复的重试次数(默认为3次)
     */
    FLINK_AUTO_RECOVERY_RETRY,

    /**
     * JSTORM TOP任务启动的最大等待时间。超过此时间认为启动失败 (秒)
     * 注意这个时间不是整个任务启动的时间，还包括45秒的yarn检查时间，和nimbus检查时间(最大30秒，重试10次)
     */
    JSTORM_TOP_START_TIMEOUT,

    /**
     * 本地的 FLINK 安装路径
     */
    FLINK_HOME,

    /**
     * FLINK_JOB 启动的最大等待时间。超过此时间认为启动失败 (秒) 默认为1分钟
     * 注意这个时间不是yarn-session的最大启动等待时间，yarn-session的最大启动等待时间为90秒。
     * 分两步启动，先启动session在启动job
     */
    FLINK_JOB_START_TIMEOUT,

    /**
     * JOB_MANGER 的内存数，不设置默认为 2048m
     */
    FLINK_JOB_MANGER_MEM,

    /**
     *  Hbase 的 zk host
     */
    HBASE_ZK_HOST,

    /**
     *  Hbase 的 zk port
     */
    HBASE_ZK_PORT,

    /**
     * Hbase 的 zk root
     */
    HBASE_ZK_ROOT,

    /**
     * 邮箱告警总开关，不设置则不告警 （1告警，0不告警）
     */
    EMAIL_ALARM_OPEN,

    /**
     * 手机告警总开关，不设置则不告警 （1告警，0不告警）
     */
    PHONE_ALARM_OPEN,

}
