package com.ucar.streamsuite.engine.business;

import backtype.storm.Config;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.ExpiredCallback;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LoadConf;

import com.alibaba.jstorm.utils.TimeCacheMap;
import com.ucar.streamsuite.common.util.SSHUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import shade.storm.org.apache.thrift.TException;
import shade.storm.org.json.simple.JSONValue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


/**
 * Description: 访问jstorm集群的业务 工具 类
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class JStormClusterBusiness  {

    private static final Logger LOGGER = LoggerFactory.getLogger(JStormClusterBusiness.class);


    protected static TimeCacheMap<String, NimbusClient> clientManager;

    static {
        clientManager = new TimeCacheMap<String, NimbusClient>(3600,
                new ExpiredCallback<String, NimbusClient>() {
                    @Override
                    public void expire(String key, NimbusClient val) {
                        LOGGER.info("Close connection of " + key);
                        val.close();
                    }
                });
    }

    /**
     * 获得nimbusClient
     * @param zkHosts
     * @param zkPort
     * @param zkRoot
     * @return
     * @throws TException
     */
    public static NimbusClient getNimBusClientWithRetry(List<String> zkHosts,Integer zkPort,String zkRoot,Integer retryTimes, Integer retrySleepSeconds) {
        // 每隔3秒尝试1次，5次都无法连接认为无法连接
        if(CollectionUtils.isEmpty(zkHosts)||zkPort==null||StringUtils.isBlank(zkRoot)){
            return null;
        }
        if(retryTimes == null){
            retryTimes = 5;
        }
        if(retrySleepSeconds == null){
            retrySleepSeconds = 3;
        }
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(retrySleepSeconds);
            } catch (InterruptedException e) {
            }
            NimbusClient nimbusClient;
            try {
                Map conf = readDefaultConfig();
                conf.put(Config.STORM_ZOOKEEPER_SERVERS,zkHosts);
                conf.put(Config.STORM_ZOOKEEPER_PORT, zkPort);
                conf.put(Config.STORM_ZOOKEEPER_ROOT, zkRoot);
                nimbusClient = NimbusClient.getConfiguredClient(conf);
                if(nimbusClient!=null){
                    return nimbusClient;
                }
            } catch (Exception e) {
                //LOGGER.error("获取nimbus client 失败! zkRoot=" + zkRoot);
            }
            if (retryTimes-- <= 0) {
                return null;
            }
        }
    }

    /**
     * 获得 TopologyInfo 带有重试机制
     * @param topId
     * @return
     */
    public static TopologyInfo getTopologyInfoWithRetry(NimbusClient nimbusClient,String topId) {
        Integer retryTimes = 3;
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
            }
            TopologyInfo topologyInfo = null;
            try {
                topologyInfo = nimbusClient.getClient().getTopologyInfo(topId);
                if(topologyInfo!=null){
                    return topologyInfo;
                }
            } catch (Throwable e) {
            }
            if (retryTimes-- <= 0) {
                return null;
            }
        }
    }

    /**
     * 判断是否存在NimbusServer进程
     * 没连上也认为进行恢复。也就是默认只要nimbusClient么有拿到就恢复。悲观考虑
     * @param host
     * @return
     * @throws Exception
     */
    public static boolean existNimbusProcess(String host,Integer port,String userName,String password){
        try{
            String rString = SSHUtil.execute(host,port,userName,password,"ps -ef|grep NimbusServer",true);
            if(StringUtils.isNotBlank(rString) && rString.indexOf("NimbusServer")!=-1 && rString.indexOf("java -server")!=-1){
                return true;
            }
            rString = SSHUtil.execute(host,port,userName,password,"ps -ef|grep NimbusServer",true);
            if(StringUtils.isNotBlank(rString) && rString.indexOf("nimbus.log")!=-1 && rString.indexOf("java -server")!=-1){
                return true;
            }
        }catch(Exception e){
            LOGGER.error("existNimbusProcess is error",e);
        }
        return false;
    }

    public static Map readStormConfig() {
        Map ret = readDefaultConfig();
        String confFile = System.getProperty("storm.conf.file");
        Map storm;
        if (StringUtils.isBlank(confFile)) {
            storm = LoadConf.findAndReadYaml("storm.yaml", false, true);
        } else {
            storm = loadDefinedConf(confFile);
        }
        ret.putAll(storm);
        ret.putAll(readCommandLineOpts());

        replaceLocalDir(ret);
        return ret;
    }

    private static Map DEFAULT_CONF = null;
    private static Map readDefaultConfig() {
        synchronized(JStormClusterBusiness.class) {
            if (DEFAULT_CONF == null) {
                DEFAULT_CONF = LoadConf.findAndReadYaml("defaults.yaml", true, true);
            }
        }
        return DEFAULT_CONF;
    }

    private static Map loadDefinedConf(String confFile) {
        File file = new File(confFile);
        if (!file.exists()) {
            return LoadConf.findAndReadYaml(confFile, true, true);
        }

        Yaml yaml = new Yaml();
        Map ret;
        try {
            ret = (Map) yaml.load(new FileReader(file));
        } catch (FileNotFoundException e) {
            ret = null;
        }
        if (ret == null)
            ret = new HashMap();

        return new HashMap(ret);
    }

    private static Map readCommandLineOpts() {
        Map ret = new HashMap();
        String commandOptions = System.getProperty("storm.options");
        if (commandOptions != null) {
            String[] configs = commandOptions.split(",");
            for (String config : configs) {
                config = URLDecoder.decode(config);
                String[] options = config.split("=", 2);
                if (options.length == 2) {
                    Object val = JSONValue.parse(options[1]);
                    if (val == null) {
                        val = options[1];
                    }
                    ret.put(options[0], val);
                }
            }
        }

        String excludeJars = System.getProperty("exclude.jars");
        if (excludeJars != null) {
            ret.put("exclude.jars", excludeJars);
        }

        /*
         * Trident and old transaction implementation do not work on batch mode. So, for the relative topology builder
         */
        String batchOptions = System.getProperty(ConfigExtension.TASK_BATCH_TUPLE);
        if (!StringUtils.isBlank(batchOptions)) {
            boolean isBatched = JStormUtils.parseBoolean(batchOptions, true);
            ConfigExtension.setTaskBatchTuple(ret, isBatched);
            System.out.println(ConfigExtension.TASK_BATCH_TUPLE + " is " + batchOptions);
        }
        String ackerOptions = System.getProperty(Config.TOPOLOGY_ACKER_EXECUTORS);
        if (!StringUtils.isBlank(ackerOptions)) {
            Integer ackerNum = JStormUtils.parseInt(ackerOptions, 0);
            ret.put(Config.TOPOLOGY_ACKER_EXECUTORS, ackerNum);
            System.out.println(Config.TOPOLOGY_ACKER_EXECUTORS + " is " + ackerNum);
        }
        return ret;
    }

    private static void replaceLocalDir(Map<Object, Object> conf) {
        String stormHome = System.getProperty("jstorm.home");
        boolean isEmpty = StringUtils.isBlank(stormHome);

        Map<Object, Object> replaceMap = new HashMap<>();

        for (Map.Entry entry : conf.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String) {
                if (StringUtils.isBlank((String) value)) {
                    continue;
                }

                String str = (String) value;
                if (isEmpty) {
                    // replace %JSTORM_HOME% as current directory
                    str = str.replace("%JSTORM_HOME%", ".");
                } else {
                    str = str.replace("%JSTORM_HOME%", stormHome);
                }

                replaceMap.put(key, str);
            }
        }

        conf.putAll(replaceMap);
    }


}
