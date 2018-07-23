
package com.ucar.streamsuite.common.hbase;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.ucar.flinkcomponent.connectors.hbase.HBaseConstant;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.ConfigKeyEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;



/**
 *  hbase 管理类，管理初始化、配置等操作
 * <br/> Created on 2014-8-5 下午4:26:22
 * @since 3.4
 */
@SuppressWarnings("unchecked")
public class HBaseManager {

    private static final ConcurrentHashMap<String,HTable> tablePool = new ConcurrentHashMap<String, HTable>();

    private static final HTableOperateInterface hbaseClient = new HTableOperatorImpl();

	private static  Configuration CONF = loadHBaseConfig();

	public static Configuration getConfiguration(){
		return CONF ;
	}

    private static String zkHostForFLinkSQL;
    private static String zkPortForFLinkSQL;
    private static String zkPrefixForFLinkSQL;

	private static Configuration loadHBaseConfig(){
		Configuration configuration = HBaseConfiguration.create();
		String zkHosts = ConfigProperty.getConfigValue(ConfigKeyEnum.HBASE_ZK_HOST);
		String zkPort = ConfigProperty.getConfigValue(ConfigKeyEnum.HBASE_ZK_PORT);
        String hbaseZkRoot = ConfigProperty.getConfigValue(ConfigKeyEnum.HBASE_ZK_ROOT);

        if(zkHostForFLinkSQL != null){
            zkHosts = zkHostForFLinkSQL;
        }

        if(zkPortForFLinkSQL != null){
            zkPort = zkPortForFLinkSQL;
        }

        if(zkPrefixForFLinkSQL != null){
            hbaseZkRoot = zkPrefixForFLinkSQL;
        }

        configuration.set("hbase.rpc.timeout","5000"); //5秒rpc超时
        configuration.set("hbase.htable.threads.max","500"); //htable最大线程
        configuration.set("hbase.client.retries.number","3");
        configuration.set("hbase.client.pause","100");
        configuration.set("zookeeper.recovery.retry","3");
        configuration.set("hbase.client.operation.timeout","30000");
        configuration.set("zookeeper.session.timeout","60000");
        configuration.set("hbase.client.keyvalue.maxsize","31457280");
        configuration.set("zookeeper.znode.parent",hbaseZkRoot);
        configuration.set("hbase.zookeeper.property.clientPort",zkPort);
        configuration.set("hbase.zookeeper.quorum",zkHosts);
		return configuration ;
	}

    public static HTable getHBaseTable(String tableName) {
        HTable hTable = tablePool.get(tableName);
        if (hTable == null) {
            synchronized (HBaseManager.class) {
                hTable = tablePool.get(tableName);
                if (hTable == null) {
                    boolean exists = hbaseClient.tableExists(tableName);
                    if (exists) {
                        hTable = new HTable(tableName);
                        hTable.setAutoFlush(true);
                        tablePool.put(tableName, hTable);
                    }
                }
            }
        }
        return hTable;
    }

    /**
     * 目前用于flink sql 插入hbase使用
     * @param properties
     */
    public static void initForFlinkSQL(Properties properties){
        zkHostForFLinkSQL = properties.getProperty(HBaseConstant.ZKHOST);
        zkPortForFLinkSQL = properties.getProperty(HBaseConstant.ZKPORT);
        zkPrefixForFLinkSQL = properties.getProperty(HBaseConstant.ZKPREFIX);

        CONF = loadHBaseConfig();
    }
}
