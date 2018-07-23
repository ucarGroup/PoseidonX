/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.streaming.config;

import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.StreamingUtils;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

/**
 * 全局配置信息操作类
 *
 */
public class StreamingConfig extends HashMap<String, Object> implements Serializable
{
    /*
     * 凡是包含*.inner.*的属性的，都是系统的内部属性，
     * 不需要暴露给用户，不需要用户进行配置
     */
    
    /*-----------------------Stream算子基本配置信息start---------------------------*/
    
    /**
     * 输入流名字配置
     */
    public static final String STREAMING_INNER_INPUT_STREAM_NAME = "streaming.inner.input.stream.name";
    
    /**
     * 输出流名字配置
     */
    public static final String STREAMING_INNER_OUTPUT_STREAM_NAME = "streaming.inner.output.stream.name";
    
    /**
     * 输入算子的schema
     */
    public static final String STREAMING_INNER_INPUT_SCHEMA = "streaming.inner.input.schema";
    
    /**
     * 输出算子的schema
     */
    public static final String STREAMING_INNER_OUTPUT_SCHEMA = "streaming.inner.output.schema";
    
    /**
     * kill任务的超时时间
     */
    public static final String STREAMING_KILLAPPLICATION_OVERTIME = "streaming.killapplication.overtime";

    /**
     * 用户文件(包含jar包)的最大大小，单位MB
     */
    public static final String STREAMING_USERFILE_MAXSIZE = "streaming.userfile.maxsize";
    /**
     * 是否是测试模式
     * 如果是测试模式，系统就会只构建拓扑，但是并不提交。
     * 主要是为了规避Netty构建的TCPServer在Maven测试的时候无法关闭的问题
     */
    public static final String STREAMING_COMMON_ISTESTMODEL = "streaming.common.istestmodel";
    
    /**
     * 输出类型
     * I,R,IR
     */
    public static final String STREAMING_COMMON_OUTPUT_TYPE = "streaming.common.output.type";
    
    /**
     * 默认的并发度
     */
    public static final String STREAMING_COMMON_PARALLEL_NUMBER = "streaming.common.parallel.number";
    
    /**
     * 底层应用程序平台发布的选择stormApplication还是s4application
     */
    public static final String STREAMING_ADAPTOR_APPLICATION = "streaming.adaptor.application";
    
    /**
     * 系统临时目录路径
     */
    public static final String STREAMING_TEMPLATE_DIRECTORY = "streaming.template.directory";

    /**
     * CQL 默认时区
     * 默认为客户端当前时区
     * 可以通过设置修改时区
     * 时区格式举例：GMT+08:00, Asia/Shanghai, America/Los_Angeles
     */
    public static final String STREAMING_OPERATOR_TIMEZONE = "streaming.operator.timezone";

    /*-----------------------Stream算子基本配置信息end---------------------------*/
    
    /*-----------------------storm相关配置属性 start---------------------------*/
    
    /**
     * 是否需要ACK
     */
    public static final String STREAMING_STORM_COMMON_ISACK = "streaming.storm.common.isack";
    
    /**
     * nimbus的地址
     */
    public static final String STREAMING_STORM_NIMBUS_HOST = "streaming.storm.nimbus.host";
    
    /**
     * nimbus的端口
     */
    public static final String STREAMING_STORM_NIMBUS_PORT = "streaming.storm.nimbus.port";
    
    /**
     * 客户端和服务端连接的thrift协议
     */
    public static final String STREAMING_STORM_THRIFT_TRANSPORT_PLUGIN = "streaming.storm.thrift.transport.plugin";
    
    /**
     * 任务提交方式，是否是本地提交
     */
    public static final String STREAMING_STORM_SUBMIT_ISLOCAL = "streaming.storm.submit.islocal";
    
    /**
     * Storm任务的worker数量
     */
    public static final String STREAMING_STORM_WORKER_NUMBER = "streaming.storm.worker.number";
    
    /**
     * Storm任务的rebalance等待时间
     */
    public static final String STREAMING_STORM_REBALANCE_WAITSECONDS = "streaming.storm.rebalance.waitseconds";
    
    /**
     * 本地任务存活时间，过了这个时间之后，就会被kill掉
     * 单位毫秒
     */
    public static final String STREAMING_LOCALTASK_ALIVETIME_MS = "streaming.localtask.alivetime.ms";
    
    /**
     * kill 任务的时候的等待时间，单位秒
     */
    public static final String STREAMING_STORM_KILLTASK_WAITSECONDS = "streaming.storm.killtask.waitseconds";
    
    /**
     * storm HA的zookeeper配置属性
     * 例如
     * 192.168.0.2,192.168.0.3,192.168.0.4
     */
    public static final String STREAMING_STORM_HA_ZKADDRESS = "streaming.storm.ha.zkaddress";
    
    /**
     * storm HA的zookeeper端口
     * 例如
     * 2181
     */
    public static final String STREAMING_STORM_HA_ZKPORT = "streaming.storm.ha.zkport";
    
    /**
     * HA连接zookeeper的session 超时时间
     */
    public static final String STREAMING_STORM_HA_ZKSESSIONTIMEOUT = "streaming.storm.ha.zksessiontimeout";
    
    /**
     * HA连接zookeeper 建立连接的超时时间
     */
    public static final String STREAMING_STORM_HA_ZKSCONNECTIONTIMEOUT = "streaming.storm.ha.zkconnectiontimeout";
    
    /*-----------------------storm相关配置属性 end---------------------------*/
    
    /*---------------------storm安全相关配置属性 start------------------------*/
    
    /**
     * Streaming是否启用安全的标志。
     * 可以使用如下几个值：NONE, KERBEROS,
     * 不区分大小写
     * 默认值是：NONE 不启用安全
     */
    public static final String STREAMING_SECURITY_AUTHENTICATION = "streaming.security.authentication";
    
    /**
     * zookeeper principal
     */
    public static final String STREAMING_SECURITY_ZOOKEEPER_PRINCIPAL = "streaming.security.zookeeper.principal";

    /**
     * 用户 principal
     * 人机账户登录的时候可以为空
     */
    public static final String STREAMING_SECURITY_USER_PRINCIPAL = "streaming.security.user.principal";

    /**
     * 用户 principal instance
     * 用于人机账号的第二段自动填充
     */
    public static final String STREAMING_SECURITY_USER_PRINCIPAL_INSTANCE = "streaming.security.user.principal.instance";

    /**
     * 用户keytab地址
     * 人机账户登录的时候，可以为空
     */
    public static final String STREAMING_SECURITY_KEYTAB_PATH = "streaming.security.keytab.path";
    
    /**
     * sasl安全级别，默认auth_conf
     */
    public static final String STREAMING_SECURITY_SASL_QOP = "streaming.security.sasl.qop";
    
    /**
     * krb5文件地址
     * 如果为空，则从系统默认地址获取
     * 默认地址获取顺序为：
     * 1、${JAVA_HOME}/lib/security/krb5.conf
     * 2、${JAVA_HOME}/krb5.ini
     * 3、windows: %windir%\krb5.ini
     *      linux: /etc/krb5.conf
     */
    public static final String STREAMING_SECURITY_KRBCONF_PATH = "streaming.security.krbconf.path";
    
    /*---------------------storm安全相关配置属性 end---------------------------*/
    
    /*-----------------------Join算子基本配置信息start---------------------------*/
    
    /**
     * 左输入流名字配置
     */
    public static final String OPERATOR_JOIN_INNER_LEFT_INPUT_STREAM_NAME =
        "operator.join.inner.left.input.stream.name";
    
    /**
     * 右输入流名字配置
     */
    public static final String OPERATOR_JOIN_INNER_RIGHT_INPUT_STREAM_NAME =
        "operator.join.inner.right.input.stream.name";
    
    /**
     * 左输入流的schema
     */
    public static final String OPERATOR_JOIN_INNER_LEFT_SCHEMA = "operator.join.inner.left.schema";
    
    /**
     * 右输入流的schema
     */
    public static final String OPERATOR_JOIN_INNER_RIGHT_SCHEMA = "operator.join.inner.right.schema";
    
    /**
     * 是否为单流JOIN
     */
    public static final String OPERATOR_JOIN_INNER_UNIDIRECTIONAL = "operator.join.inner.unidirectional";
    
    /**
     * 单流的触发流索引，0为左流，1为右流
     */
    public static final String OPERATOR_JOIN_INNER_UNIDIRECTIONAL_INDEX = "operator.join.inner.unidirectional.index";
    
    /*-----------------------SelfJoin算子基本配置信息start---------------------------*/
    
    /**
     * 输入右流名称
     */
    public static final String OPERATOR_SELFJOIN_INNER_LEFT_INPUT_STREAM_NAME =
        "operator.selfjoin.inner.left.input.stream.name";
    
    /**
     * 输入右流名称
     */
    public static final String OPERATOR_SELFJOIN_INNER_RIGHT_INPUT_STREAM_NAME =
        "operator.selfjoin.inner.right.input.stream.name";
    
    /**
     * 输入单流的schema
     */
    public static final String OPERATOR_SELFJOIN_INNER_INPUT_SCHEMA = "operator.selfjoin.inner.input.schema";
    
    /**
     * 是否为单流JOIN
     */
    public static final String OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL = "operator.selfjoin.inner.unidirectional";
    
    /**
     * 单流的触发流索引，0为左流，1为右流
     */
    public static final String OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL_INDEX =
        "operator.selfjoin.inner.unidirectional.index";
    
    /*-----------------------SelfJoin算子基本配置信息end---------------------------*/
    
    /*-----------------------TCP相关配置信息start---------------------------*/
    
    /**
     * 远程tcp服务器地址
     * 只允许有一个地址
     */
    public static final String OPERATOR_TCPCLIENT_SERVER = "operator.tcpclient.server";
    
    /**
     * 待连接的远程tcp服务器端口
     */
    public static final String OPERATOR_TCPCLIENT_PORT = "operator.tcpclient.port";
    
    /**
     * tcpsession超时时间
     * 单位：毫秒
     */
    public static final String OPERATOR_TCPCLIENT_SESSIONTIMEOUT = "operator.tcpclient.sessiontimeout";

    /**
     * 数据包中每行数据的长度
     * 允许将多个数据包一起合并发送。
     */
    public static final String OPERATOR_TCPCLIENT_PACKAGELENGTH = "operator.tcpclient.packagelength";
    /*-----------------------TCP相关配置信息end---------------------------*/
    

    /*-------------------------kafka相关配置信息-----------------------*/

    /**
     * kafka的zookeeper连接的配置参数
     */
    public static final String OPERATOR_KAFKA_ZOOKEEPERS = "operator.kafka.zookeepers";

    /**
     * kafka客户端的id配置参数
     */
    public static final String OPERATOR_KAFKA_GROUPID = "operator.kafka.groupid";

    /**
     * 序列化类配置参数
     */
    public static final String OPERATOR_KAFKA_MESSAGESERIALIZERCLASS = "operator.kafka.messageserializerclass";

    /**
     * zookeeper的session有效时间
     * 单位：毫秒
     */
    public static final String OPERATOR_KAFKA_ZKSESSIONTIMEOUT = "operator.kafka.zksessiontimeout";

    /**
     * kafka的zk.synctime.ms参数
     * 单位：毫秒
     */
    public static final String OPERATOR_KAFKA_ZKSYNCTIME = "operator.kafka.zksynctime";

    /**
     * kafka的metadata.broker.list参数
     */
    public static final String OPERATOR_KAFKA_BROKERS = "operator.kafka.brokers";

    /**
     * kafka的topic参数
     */
    public static final String OPERATOR_KAFKA_TOPIC = "operator.kafka.topic";


    /**
     * 从头开始读取kafka数据
     */
    public static final String OPERATOR_KAFKA_READ_FROMBEGINNING = "operator.kafka.read.frombeginning";


    /*-------------------------kafka相关配置信息end-----------------------*/




    /*-------------------------MetaQ相关配置信息-----------------------*/

    /**
     * MetaQ的zookeeper连接的配置参数
     */
    public static final String OPERATOR_METAQ_ZOOKEEPERS = "operator.metaq.zookeepers";

    /**
     * MetaQ客户端的id配置参数
     */
    public static final String OPERATOR_METAQ_GROUPID = "operator.metaq.groupid";

    /**
     * MetaQ的prefix参数
     */
    public static final String OPERATOR_METAQ_PREFIX = "operator.metaq.prefix";

    /**
     * MetaQ的topic参数
     */
    public static final String OPERATOR_METAQ_TOPIC = "operator.metaq.topic";

    /**
     * MetaQ的topic参数
     */
    public static final String OPERATOR_METAQ_HASHFILEDS = "operator.metaq.hashfileds";

    /**
     * 从头开始读取kafka数据
     */
    public static final String OPERATOR_METAQ_READ_FROMBEGINNING = "operator.metaq.read.frombeginning";


    /*-------------------------MetaQ相关配置信息end-----------------------*/




    /*------------------------HeadStream相关配置信息start---------------*/
    
    /**
     * 时间单位
     */
    public static final String OPERATOR_HEADSTREAM_TIMEUNIT = "operator.headstream.timeunit";
    
    /**
     * 每周期发送事件数量
     */
    public static final String OPERATOR_HEADSTREAM_EVENTNUMPERPERIOD = "operator.headstream.eventnumperperiod";
    
    /**
     * 时间周期
     */
    public static final String OPERATOR_HEADSTREAM_PERIOD = "operator.headstream.period";
    
    /**
     * 是否使用周期发送
     */
    public static final String OPERATOR_HEADSTREAM_ISSCHEDULE = "operator.headstream.isschedule";
    
    /**
     * 限制发送事件数
     */
    public static final String OPERATOR_HEADSTREAM_TOTALNUMBER = "operator.headstream.totalnumber";
    
    /**
     * 第一个事件延迟发送时间
     */
    public static final String OPERATOR_HEADSTREAM_DELAYTIME = "operator.headstream.delaytime";
    
    /*------------------------HeadStream相关配置信息end---------------*/
    
    /*------------------------ConsolePrint相关配置信息start---------------*/
    
    /**
     * 计数频率
     */
    public static final String OPERATOR_CONSOLEPRINT_FREQUENCE = "operator.consoleprint.frequence";
    
    /*------------------------ConsolePrint相关配置信息end---------------*/
    
    /**
     * CQL系统默认的序列化和反序列化类
     */
    public static final String STREAMING_SERDE_DEFAULT = "streaming.serde.default";
    
    /*------------------------SimpleSerde相关配置属性 start---------------*/
    
    /**
     * 消息序列化和反序列化的分隔符
     */
    public static final String SERDE_SIMPLESERDE_SEPARATOR = "serde.simpleserde.separator";
    
    /**
     * keyvalue 格式分隔符
     */
    public static final String SERDE_KEYVALUESERDE_SEPARATOR = "serde.keyvalueserde.separator";
    
    /*------------------------SimpleSerde相关配置信息end---------------*/
    
    /*------------------------bianry Serde 相关配置属性 start---------------*/
    
    /**
     * 每列字节长度的数组
     */
    public static final String SERDE_BINARYSERDE_ATTRIBUTESLENGTH = "serde.binaryserde.attributeslength";
    
    /**
     * 时间类型表示方法
     * String或者Long类型，默认Long类型
     * 不区分大小写
     */
    public static final String SERDE_BINARYSERDE_TIMETYPE = "serde.binaryserde.timetype";

    /**
     * decimal类型表示方法
     * String或者decimal类型，默认decimal类型
     * 不区分大小写
     */
    public static final String SERDE_BINARYSERDE_DECIMALYPE = "serde.binaryserde.decimaltype";

    /*------------------------bianry Serde 相关配置信息end----------------*/
    
    /*------------------------Union相关配置属性 start-------------------*/
    
    /**
     * Union输入流名称及取值表达式列表
     */
    public static final String OPERATOR_UNION_INNER_INPUTNAMES_AND_EXPRESSION =
        "operator.union.inner.inputnames.and.expression";
    
    /**
     * Union输入流名称及对应类型列表
     */
    public static final String OPERATOR_UNION_INPUTNAMES_AND_SCHEMA = "operator.union.inputnames.and.schema";
    
    /*------------------------Union相关配置属性 end--------------------*/
    
    /*----------------------------------Combine算子相关配置属性start-----------------------------*/
    
    /**
     * Combine输入流名称列表
     */
    public static final String OPERATOR_COMBINE_INPUTNAMES = "operator.combine.inputnames";
    
    /**
     * Combine输入流名称及各个流合并所用的key
     */
    public static final String OPERATOR_COMBINE_INPUTNAMES_AND_KEY = "operator.combine.inputnames.and.key";
    
    /**
     * Combine输入流名称及取值表达式列表
     */
    public static final String OPERATOR_COMBINE_INPUTNAMES_AND_EXPRESSION =
        "operator.combine.inputnames.and.expression";
    
    /**
     * Combine输入流名称及对应类型列表
     */
    public static final String OPERATOR_COMBINE_INPUTNAMES_AND_SCHEMA = "operator.combine.inputnames.and.schema";
    
    /*----------------------------------Combine算子相关配置属性end-------------------------------*/
    
    /*------------------------数据源相关配置属性 start---------------*/
    
    /**
     * RDB数据源驱动名称
     */
    public static final String DATASOURCE_RDB_DRIVER = "datasource.rdb.driver";
    
    /**
     * RDB数据库连接URL
     */
    public static final String DATASOURCE_RDB_URL = "datasource.rdb.url";
    
    /**
     * RDB数据库用户名
     */
    public static final String DATASOURCE_RDB_USERNAME = "datasource.rdb.username";
    
    /**
     * RDB数据库用户密码
     */
    public static final String DATASOURCE_RDB_PASSWORD = "datasource.rdb.password";

    /**
     * 数据库查询密码解密
     */
    public static final String DATASOURCE_RDB_DECRYPTCLASS = "datasource.rdb.decryptclass";

    /**
     * 数据库加密类型
     * USER,PASSWORD,NONE,ALL
     */
    public static final String DATASOURCE_RDB_DECRYPTTYPE = "datasource.rdb.decrypttype";

    /*-------------------------数据源相关配置属性  end---------------*/
    
    /*------------------------数据源相关配置属性 start---------------*/
    
    /**
     * TCP Server 数据读取算子
     */
    public static final String OPERATOR_TCPSERVER_PORT = "operator.tcpserver.port";
    
    /**
     * 数据包长度
     */
    public static final String OPERATOR_TCPSERVER_FIXEDLENGTH = "operator.tcpserver.fixedlength";
    
    /*-------------------------数据源相关配置属性  end---------------*/
    
    /**
     * 配置属性序列化成xml的时候，列表的转义名称
     */
    public static final String LIST_ALIAS = "configuration";
    
    /**
     * 默认配置项，从默认配置文件中读取
     */
    private static final Map<String, Object> DEFAULT_CONFIG = Maps.newHashMap();
    
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 2170263655525853424L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(StreamingConfig.class);
    
    /**
     * 默认配置文件
     * 由于这种配置属性的写法没有默认值，所以只能将这些默认值放在这个配置文件当中
     */
    private static final String DEFAULT_CONFIG_FILE = "streaming-default.xml";
    
    /**
     * 系统配置文件
     */
    private static final String STREAING_CONFIG_FILE = "streaming-site.xml";
    
    /**
     * 初始化默认配置信息
     */
    static
    {
        DEFAULT_CONFIG.putAll(readConfigFile(DEFAULT_CONFIG_FILE));
    }
    
    /**
     * <默认构造函数>
     */
    public StreamingConfig()
    {
        loadDefaultConfig();
        this.putAll(readConfigFile(STREAING_CONFIG_FILE));
    }
    
    private static Map<String, Object> readConfigFile(String configFile)
    {
        Map<String, Object> configMap = Maps.newHashMap();
        List<ConfigBean> configBeanList = Lists.newArrayList();
        configBeanList = readConfsInFile(configFile, configBeanList);
        for (ConfigBean configBean : configBeanList)
        {
            try
            {
                configMap.put(configBean.getName(),
                    ConfVariable.getValue(new ConfVariable(configBean.getValue()), configMap, null));
            }
            catch (StreamingException e)
            {
                LOG.warn("Ignore a StreamingException");
            }
        }
        return configMap;
    }
    
    @SuppressWarnings("unchecked")
    private static List<ConfigBean> readConfsInFile(String configFile, List<ConfigBean> configBeanList)
    {
        InputStream stream = null;
        try
        {
            ClassLoader classLoader = StreamingConfig.class.getClassLoader();
            if(classLoader == null )
            {
                LOG.warn("can't found streaming-site.xml");
                return configBeanList;
            }

            stream = classLoader.getResourceAsStream(configFile);
            if(stream == null)
            {
                LOG.warn("can't found streaming-site.xml");
                return configBeanList;
            }
            XStream xstream = getXStream();
            xstream.alias(LIST_ALIAS, List.class);
            configBeanList = (List<ConfigBean>)xstream.fromXML(stream);
        }
        catch (Exception e)
        {
            LOG.warn("can't found streaming-site.xml");
        }
        finally
        {
            StreamingUtils.close(stream);
        }
        return configBeanList;
    }
    
    /**
     * 获取 XML对象
     *
     */
    private static XStream getXStream()
    {
        XStream xstream = new XStream(new DomDriver());
        xstream.alias("property", ConfigBean.class);
        return xstream;
    }
    
    /**
     * <拷贝默认参数>
     * <拷贝默认参数>
     *
     */
    private Map<String, Object> defaultClone()
    {
        Map<String, Object> target = Maps.newHashMap();
        for (Iterator< ? > keyIt = DEFAULT_CONFIG.keySet().iterator(); keyIt.hasNext();)
        {
            String key = (String)keyIt.next();
            target.put(key, DEFAULT_CONFIG.get(key));
        }
        return target;
    }
    
    /**
     * 获取指定参数类型
     */
    public int getIntValue(String key)
        throws StreamingException
    {
        if (this.containsKey(key))
        {
            String strValue = get(key).toString();
            try
            {
                return Integer.valueOf(strValue);
            }
            catch (NumberFormatException e)
            {
                StreamingException exception= new StreamingException(ErrorCode.CONFIG_FORMAT, strValue, "int");
                LOG.error(ErrorCode.CONFIG_FORMAT.getFullMessage(strValue, "int"));
                throw exception;
            }
        }
        else
        {
            StreamingException exception = new StreamingException(ErrorCode.CONFIG_NOT_FOUND, key);
            LOG.error(ErrorCode.CONFIG_NOT_FOUND.getFullMessage(key));
            throw exception;
        }
    }
    
    /**
     * 获取指定参数类型
     */
    public long getLongValue(String key)
        throws StreamingException
    {
        if (this.containsKey(key))
        {
            try
            {
                return Long.valueOf(get(key).toString());
            }
            catch (NumberFormatException e)
            {
                StreamingException exception= new StreamingException(ErrorCode.CONFIG_FORMAT, get(key).toString(), "long");
                LOG.error(ErrorCode.CONFIG_FORMAT.getFullMessage(get(key).toString(), "long"));
                throw exception;
            }
        }
        else
        {
            StreamingException exception = new StreamingException(ErrorCode.CONFIG_NOT_FOUND, key);
            LOG.error(ErrorCode.CONFIG_NOT_FOUND.getFullMessage(key));
            throw exception;
        }
    }
    
    /**
     * 获取指定参数类型
     */
    public double getDoubleValue(String key)
        throws StreamingException
    {
        if (this.containsKey(key))
        {
            try
            {
                return Double.valueOf(get(key).toString());
            }
            catch (NumberFormatException e)
            {
                StreamingException exception= new StreamingException(ErrorCode.CONFIG_FORMAT, get(key).toString(), "double");
                LOG.error(ErrorCode.CONFIG_FORMAT.getFullMessage(get(key).toString(), "double"));
                throw exception;
            }
        }
        else
        {
            StreamingException exception = new StreamingException(ErrorCode.CONFIG_NOT_FOUND, key);
            LOG.error(ErrorCode.CONFIG_NOT_FOUND.getFullMessage(key));
            throw exception;
        }
    }
    
    /**
     * 获取指定参数类型
     */
    public float getFloatValue(String key)
        throws StreamingException
    {
        if (this.containsKey(key))
        {
            try
            {
                return Float.valueOf(get(key).toString());
            }
            catch (NumberFormatException e)
            {
                StreamingException exception= new StreamingException(ErrorCode.CONFIG_FORMAT, get(key).toString(), "float");
                LOG.error(ErrorCode.CONFIG_FORMAT.getFullMessage(get(key).toString(), "float"));
                throw exception;
            }
        }
        else
        {
            StreamingException exception = new StreamingException(ErrorCode.CONFIG_NOT_FOUND, key);
            LOG.error(ErrorCode.CONFIG_NOT_FOUND.getFullMessage(key));
            throw exception;
        }
    }
    
    /**
     * 获取指定参数类型
     */
    public String getStringValue(String key)
        throws StreamingException
    {
        if (this.containsKey(key))
        {
            return get(key).toString();
        }
        else
        {
            StreamingException exception = new StreamingException(ErrorCode.CONFIG_NOT_FOUND, key);
            LOG.error(ErrorCode.CONFIG_NOT_FOUND.getFullMessage(key));
            throw exception;
        }
    }
    
    /**
     * 获取指定参数类型
     */
    public boolean getBooleanValue(String key)
        throws StreamingException
    {
        if (this.containsKey(key))
        {
            try
            {
                return Boolean.valueOf(get(key).toString());
            }
            catch (Exception e)
            {
                StreamingException exception= new StreamingException(ErrorCode.CONFIG_FORMAT, get(key).toString(), "boolean");
                LOG.error(ErrorCode.CONFIG_FORMAT.getFullMessage(get(key).toString(), "boolean"));
                throw exception;
            }
        }
        else
        {
            StreamingException exception = new StreamingException(ErrorCode.CONFIG_NOT_FOUND, key);
            LOG.error(ErrorCode.CONFIG_NOT_FOUND.getFullMessage(key));
            throw exception;
        }
    }
    
    /**
     * 将系统的默认属性加载到配置文件当中
     * 这里属性的之必须是完全拷贝，防止默认值被外部配置属性修改
     *
     */
    private void loadDefaultConfig()
    {
        this.putAll(defaultClone());
    }


      /*------------------------- Redis 相关配置信息-----------------------*/
      public static final String REDIS_GROUPNAME = "groupName";
      public static final String REDIS_CONFIG = "redisConfig";
      public static final String REDIS_ZKSERVERS = "zkservers";
      public static final String REDIS_ZKPREFIX = "zkprefix";
      public static final String REDIS_NAMESPACE = "namespace";
      public static final String REDIS_EXPIRY = "expiry";
      /*------------------------- Redis 相关配置信息-----------------------*/

}
