package com.ucar.streamsuite.yarn.appmaster;

/**
 * jstorm AM 使用的常数信息
 */
public class JstormAMConstant {

    //设置可用端口范围
    //所有的端口都在这个范围内
    public static final String AM_PORT_RANGE = "19111-19300";

    //supervisor端口范围
    public static final int SUPERVISOR_MIN_PORT = 19301;
    public static final int SUPERVISOR_MAX_PORT = 19999;

    //nimbusr端口范围
    public static final int NIMBUS_MIN_PORT = 30001;
    public static final int NIMBUS_MAX_PORT = 30501;

    //logViewPort端口范围
    public static final int LOGVIEW_MIN_PORT = 30501;
    public static final int LOGVIEW_MAX_PORT = 31001;

    public static final int CONTAINER_TYPE_NIMBUS = 0;
    public static final int CONTAINER_TYPE_SUPERVISOR = 1;


    public static final int HEARTBEAT_TIME_INTERVAL = 20*1000;

    public static final Integer JOIN_THREAD_TIMEOUT = 10*1000;

    //jstorm 集群文件在container的软路径
    public static final String JSTORM_SOFTLINK="jstorm-2.2.1";

    public static final String NAME_NIMBUS = "nimbus";
    public static final String NAME_SUPERVISOR = "supervisor";

    //container 执行的环境变量 - PATH
    public static final String ENV_PATH = "/usr/java/jdk1.7.0_79/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin:/sbin:/usr/sbin:/root/bin";

    public static final int AM_RM_CLIENT_INTERVAL = 1000;

    public static final Integer EXIT_SUCCESS = 0;
    public static final Integer EXIT_FAIL = -1;
    public static final Integer EXIT_FAIL1 = 1;
    public static final Integer EXIT_FAIL2 = 2;

    public static final String LOG_PATH="/data/log/hadoop/";

    public static final String REGISTRY_JSTORM_YARN = "registry_poseidon_jstorm_yarn";
    public static final String REGISTYR_GLOBAL_HOST="global_host";
    public static final String REGISTYR_APP_TYPE="registry_poseidon_app_type";
    public static final String REGISTRY_HOST="host";
    public static final String REGISTRY_PORT="port";
    public static final String REGISTRY_CONTAINER_PORT="container_port";
    public static final String REGISTRY_CONTAINER="container";
    public static final String REGISTRY_HTTP = "http";
    public static final String REGISTRY_HOST_PORT = "host/port";
    public static final String REGISTRY_RPC = "rpc";
    public static final String REGISTRY_AM = "JSTORM_AM";
    public static final String REGISTRY_JSTORM_HOST = "jstorm_host";
    public static final String REGISTRY_JSTORM_NIMBUS_PORT = "jstorm_nimbus_port";
    public static final String REGISTRY_JSTORM_LOG_PORT = "jstorm_log_port";
    public static final String REGISTRY_JSTORM_TYPE = "jstorm_type";
    public static final String REGISTRY_JSTORM_LOCAL_DIR = "jstorm_local_dir";
    public static final String REGISTRY_JSTORM_CONTAINER = "jstorm_container";
    public static final String REGISTRY_JSTORM_ZKHOST = "jstorm_zkhost";
    public static final String REGISTRY_JSTORM_ZKPORT = "jstorm_zkport";
    public static final String REGISTRY_JSTORM_ZKROOT = "jstorm_zkroot";
    public static final String REGISTRY_ROOT = "streamSuite";
    public static final String REGISTRY_NIMBUS_PORT = "nimbus_port";
    public static final String REGISTRY_SUPERVISOR_PORT_LIST = "supervisor_port_list";
    public static final String REGISTRY_KILLED="killed";
    public static final String REGISTRY_STATUS="status";
    public static final String REGISTRY_STATUS_RUNNING="running";
    public static final String REGISTRY_STATUS_STOPED="stoped";
    public static final String REGISTRY_TYPE_NIMBUS="nimbus";
    public static final String REGISTRY_TYPE_SUPERVISOR="supervisor";
    public static final String REGISTRY_TYPE_LOGVIEW="logview";
}
