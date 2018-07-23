package com.ucar.streamsuite.common.constant;

import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.util.YarnClientProxy;

/**
 * Description: 常量类
 * Created on 2018/1/30 上午10:52
 *
 */
public class StreamContant {

    //session和cookie中存储用户信息的key
    public final static String SESSION_USER = "stream_user";
    public final static String SESSION_USER_ROLE = "stream_user_role";
    public static final String UNION_PLATFORM_SESSION_KEY = "ldap-user";

    //包上传目录 (HDFS)
    public static final String HDFS_PROJECT_PACKAGE_ROOT = "/streamsuitejar/projectversion/";  //项目包
    public static final String HDFS_SYS_PACKAGE_ROOT = "/streamsuitejar/jstormversions/";   //jstorm包
    public static final String HDFS_AM_PACKAGE_ROOT = "/streamsuitejar/amversions/";   //系统包

    //访问hdfs的配置
    public static final String HADOOP_USER_NAME = "hadoop";
    public static final String HDFS_HADOOP_ZOOKEEPER= YarnClientProxy.getConf().get("yarn.resourcemanager.zk-address");


    // streamSuit 两台机器选主的
    public static final String ZOOKEEPER_LEADER_DIR = "/streamSuite/leader";
    public static String LOCAL_JAR_TMP_PATH = ConfigProperty.getConfigValue(ConfigKeyEnum.LOCAL_PROJECT_ITEM_DIR);
}
