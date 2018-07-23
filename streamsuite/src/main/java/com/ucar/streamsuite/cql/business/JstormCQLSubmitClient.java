package com.ucar.streamsuite.cql.business;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;

import com.google.common.collect.Lists;
import com.ucar.streamsuite.cql.dto.JstormCqlTaskDTO;

import com.huawei.streaming.cql.CQLClient;
import com.huawei.streaming.cql.CQLSessionState;
import com.ucar.streamsuite.engine.constants.EngineContant;
import org.apache.commons.lang.StringUtils;


/**
 * Created on 2017/1/23 上午10:18:18
 *
 */
public class JstormCQLSubmitClient {

    /**
     * jstorm 属性的名字
     * 用于替代配置文件的配置用的
     */
    private static final String STORM_CLUSTER_NAME = "clusterName";
    private static final String STORM_ZOOKEEPER_SERVERS_STR = "storm.zookeeper.servers.str";//rtcp自己用的
    private static final String STORM_ZOOKEEPER_SERVERS = "storm.zookeeper.servers";
    private static final String STORM_ZOOKEEPER_PORT = "storm.zookeeper.port";
    private static final String STORM_ZOOKEEPER_ROOT = "storm.zookeeper.root";
    private static final String TOPOLOGY_WORKERS = "topology.workers";
    private static final String WORKER_GC_CHILDOPTS = "worker.gc.childopts";
    private static final String WORKER_MEMORY_SIZE = "worker.memory.size";
    private static final String ZKADDRESS_SEPRATOR = ",";

    public static String submitCQL(JstormCqlTaskDTO cqlTaskDTO){

        String checkResult = "";

        //屏蔽无用的日志日志
        CQLClient.disableLog4j();
        CQLClient client = new CQLClient();
        client.getCustomizedConfigurationMap().put(EngineContant.RTCP_CQL_IS_SUBMIT,null);
        client.setCustomizedConfigurationMap(generateCustomConf(cqlTaskDTO));
        //设置cql对应的jar，以后提交cql任务就不用定制目标jstorm集群了
        client.getCustomizedConfigurationMap().put(EngineContant.RTCP_CQL_JAR_PATH_NAME,cqlTaskDTO.getProjectJarPath());
        //yarn环境无需指定机器
        client.getCustomizedConfigurationMap().put(Config.ISOLATION_SCHEDULER_MACHINES, null);
        if (client.initSessionState() != CQLSessionState.STATE_OK)
        {
            checkResult = "init cql session error";
            return checkResult;
        }
        List<String> sqls = CQLUtils.analyzeContent(cqlTaskDTO.getTaskCql());
        String errorCQL = "";
        for (int i = 0; i < sqls.size(); i++)
        {
            String tmp = client.checkCQL(sqls.get(i));
            if(tmp.length() >0  ) {
                tmp += "<br/>";
                checkResult += tmp;
                if(!sqls.get(i).trim().toLowerCase().startsWith("submit")) {
                    errorCQL = "错误CQL语句:\n" + sqls.get(i);
                }

            }
        }
        if(checkResult.length() == 0){
            checkResult = "任务提交成功!";
        } else {
            checkResult = checkResult.replaceAll("\n", "<br/>");
            errorCQL = errorCQL.replaceAll("\n", "<br/>");
            checkResult = errorCQL + "<br/>" + checkResult ;
        }
        return checkResult;
    }

    /**
     * 根据 CqlTaskDTO 生成自定义属性
     * @param cqlTaskDTO
     * @return
     */
    private static Map<String,Object> generateCustomConf(JstormCqlTaskDTO cqlTaskDTO) {
        Map<String,Object> map = new HashMap<String,Object>();

        if(!StringUtils.isBlank(cqlTaskDTO.getTaskName())){
            map.put(STORM_CLUSTER_NAME,cqlTaskDTO.getTaskName());
        }

        if(!StringUtils.isBlank(cqlTaskDTO.getZkServers())){
            map.put(STORM_ZOOKEEPER_SERVERS_STR,cqlTaskDTO.getZkServers());
            map.put(STORM_ZOOKEEPER_SERVERS, Lists.newArrayList(cqlTaskDTO.getZkServers().split(ZKADDRESS_SEPRATOR)));
        }

        if(!StringUtils.isBlank(cqlTaskDTO.getZkPort())){
            map.put(STORM_ZOOKEEPER_PORT,cqlTaskDTO.getZkPort());
        }

        if(!StringUtils.isBlank(cqlTaskDTO.getZkRoot())){
            map.put(STORM_ZOOKEEPER_ROOT,cqlTaskDTO.getZkRoot());
        }

        if(!StringUtils.isBlank(cqlTaskDTO.getWorkers())){
            map.put(TOPOLOGY_WORKERS,cqlTaskDTO.getWorkers());
        }

        if(!StringUtils.isBlank(cqlTaskDTO.getWorkerGcOpts())){
            map.put(WORKER_GC_CHILDOPTS,cqlTaskDTO.getWorkerGcOpts());
        }

        if(!StringUtils.isBlank(cqlTaskDTO.getWorkerMemory())){
            map.put(WORKER_MEMORY_SIZE,cqlTaskDTO.getWorkerMemory());
        }
        return map;
    }
}

