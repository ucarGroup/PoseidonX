package com.ucar.streamsuite.engine.business;

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;

import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Sets;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.ConfigKeyEnum;
import com.ucar.streamsuite.common.constant.StreamContant;

import com.ucar.streamsuite.common.util.HdfsClientProxy;

import com.ucar.streamsuite.cql.business.JstormCQLSubmitClient;
import com.ucar.streamsuite.cql.dto.JstormCqlTaskDTO;
import com.ucar.streamsuite.engine.constants.EngineContant;
import com.ucar.streamsuite.engine.dto.*;
import com.ucar.streamsuite.plugin.JstormParam;
import com.ucar.streamsuite.plugin.Base64;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;

import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;


/**
 * Description: 访问jstorm集群的业务类
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class JStormTopologyBusiness {

    private static final Logger LOGGER = LoggerFactory.getLogger(JStormTopologyBusiness.class);

    private final static ExecutorService printMessageService = Executors.newFixedThreadPool(3);

    /**
     * 返回指定 jstorm集群上面的 拓扑任务信息列表
     * @param nimbusClient
     * @return
     */
    public static JstormTopologyDTO buildeTopologyDto(ClusterSummary clusterSummary,String topId){
        if(StringUtils.isBlank(topId)){
            return null;
        }
        List<JstormTopologyDTO> jstormTopologyDTOs = buildeTopologyDtos(clusterSummary);
        if(CollectionUtils.isEmpty(jstormTopologyDTOs)){
            return null;
        }
        for (JstormTopologyDTO topologyDTO : jstormTopologyDTOs){
            if(topId.equals( topologyDTO.getTopId())){
                return topologyDTO;
            }
        }
        return null;
    }

    /**
     * 返回指定 jstorm集群上面的 拓扑任务信息列表
     * @param nimbusClient
     * @return
     */
    public static List<JstormTopologyDTO> buildeTopologyDtos(ClusterSummary clusterSummary){
        if(clusterSummary == null){
            return new ArrayList<JstormTopologyDTO>();
        }
        List<JstormTopologyDTO> entityList = new ArrayList<JstormTopologyDTO>();
        for (TopologySummary summary : clusterSummary.get_topologies()){
            JstormTopologyDTO entity = new JstormTopologyDTO(summary.get_id(), summary.get_name(), summary.get_status(),
                    summary.get_uptimeSecs(), summary.get_numTasks(), summary.get_numWorkers(), summary.get_errorInfo());
            entityList.add(entity);
        }
        return entityList;
    }

    /**
     * 返回拓扑错误信息
     * @param nimbusClient
     * @return
     */
    public static Set<String> buildTopologyErrorInfo(TopologyInfo topologyInfo){
        Set<String> error = Sets.newHashSet();
        if(topologyInfo == null){
            return error;
        }
        for (TaskSummary ts : topologyInfo.get_tasks()) {
            for (ErrorInfo info : ts.get_errors()){
                error.add(info.get_error());
            }
        }
        return error;
    }

    /**
     * 返回拓扑的组件基本信息
     * @param topologyInfo
     * @return
     */
    public static  List<JstormCompenentDTO> buildComponentInfo(TopologyInfo topologyInfo,boolean withTaskInfo){
        List<JstormCompenentDTO> entityList = new ArrayList<JstormCompenentDTO>();
        if(topologyInfo == null){
            return entityList;
        }
        try {
            List<ComponentSummary> componentSummary = topologyInfo.get_components();
            for (ComponentSummary summary : componentSummary) {
                if(summary.get_name().equals(EngineContant.COMPONENT_ACKER_NAME) || summary.get_name().equals(EngineContant.COMPONENT_TOPOLOGY_MASTER_NAME)){
                    continue;
                }
                JstormCompenentDTO entity = new JstormCompenentDTO(summary.get_name(),summary.get_type(),summary.get_errors());

                if(withTaskInfo){
                    List<JstormCompenentTaskDTO> compenentTaskDTOs = JStormTopologyBusiness.buildComponentTaskInfo(topologyInfo,summary.get_name());
                    entity.setWorkersByTaskDtos(compenentTaskDTOs);

                    //计算active的和starting任务数
                    int i = 0;
                    int j = 0;
                    for(JstormCompenentTaskDTO compenentTaskDTO:compenentTaskDTOs){
                        if(compenentTaskDTO.getStatus().toLowerCase().equals(EngineContant.WORKER_STATUS_ACTIVE)){
                            i++;
                        }
                        if(compenentTaskDTO.getStatus().toLowerCase().equals(EngineContant.WORKER_STATUS_STARTING)){
                            j++;
                        }
                    }
                    entity.setTasksActiveCount(i);
                    entity.setTasksStartingCount(j);
                }
                entityList.add(entity);
            }
            return entityList;
        } catch (Exception e) {
        }
        return entityList;
    }

    /**
     * 返回拓扑的组件任务基本信息
     * @param topologyInfo
     * @return
     */
    public static  List<JstormCompenentTaskDTO> buildComponentTaskInfo(TopologyInfo topologyInfo, String componentName){
        TreeMap<Integer, JstormCompenentTaskDTO> tasks = new TreeMap<Integer, JstormCompenentTaskDTO>();

        for (ComponentSummary cs : topologyInfo.get_components()) {
            String compName = cs.get_name();
            if (componentName.equals(compName)) {
                for (int id : cs.get_taskIds()) {
                    JstormCompenentTaskDTO jstormCompenentTaskDTO = new JstormCompenentTaskDTO();
                    jstormCompenentTaskDTO.setId(id);
                    jstormCompenentTaskDTO.setComponent(componentName);
                    tasks.put(id,jstormCompenentTaskDTO);
                }
            }
        }

        for (TaskSummary ts : topologyInfo.get_tasks()) {
            if (tasks.containsKey(ts.get_taskId())) {
                JstormCompenentTaskDTO te = tasks.get(ts.get_taskId());
                te.setId(ts.get_taskId());
                te.setHost(ts.get_host());
                te.setPort(ts.get_port());
                te.setStatus(ts.get_status());
                te.setUptimeSeconds(ts.get_uptime());
                te.setErrors(ts.get_errors());
            }
        }

        return new ArrayList<JstormCompenentTaskDTO>(tasks.values());
    }

    /**
     * 返回拓扑的任务基本信息
     * @param topologyInfo
     * @return
     */
    public static Map<Integer, JstormCompenentTaskDTO> buildTaskInfo(TopologyInfo topologyInfo){
        Map<Integer, JstormCompenentTaskDTO> tasks = new HashMap<Integer, JstormCompenentTaskDTO>();
        for (TaskSummary ts : topologyInfo.get_tasks()) {
            if (!tasks.containsKey(ts.get_taskId())) {
                JstormCompenentTaskDTO te = new JstormCompenentTaskDTO();
                te.setId(ts.get_taskId());
                te.setHost(ts.get_host());
                te.setPort(ts.get_port());
                te.setStatus(ts.get_status());
                te.setUptimeSeconds(ts.get_uptime());
                te.setErrors(ts.get_errors());
                tasks.put(ts.get_taskId(),te);
            }else{
                JstormCompenentTaskDTO te = tasks.get(ts.get_taskId());
                te.setId(ts.get_taskId());
                te.setHost(ts.get_host());
                te.setPort(ts.get_port());
                te.setStatus(ts.get_status());
                te.setUptimeSeconds(ts.get_uptime());
                te.setErrors(ts.get_errors());
            }
        }
        return tasks;
    }

    /**
     * 提交项目任务拓扑 并返回执行结果消息 String[0] 为输出的消息， String[1] 为异常消息
     * @return
     */
    public static void startTopology4Package(JstormTaskConfigDTO taskConfigDTO) throws Exception{
        try{
            //替换掉根
            String jarName= taskConfigDTO.getProjectJarPath().replace(StreamContant.HDFS_PROJECT_PACKAGE_ROOT,"");
            String localJarFilePath = ConfigProperty.getConfigValue(ConfigKeyEnum.LOCAL_PROJECT_ITEM_DIR)+ jarName;
            if(!new File(localJarFilePath).exists()){
                FileUtils.touch(new File(localJarFilePath));
                HdfsClientProxy.downloadFileToLocal(localJarFilePath,taskConfigDTO.getProjectJarPath());
            }

            JstormParam jp= new JstormParam();
            jp.setZkroot(taskConfigDTO.getJstormZkRoot());
            jp.setZkAddress(StringUtils.join(taskConfigDTO.getJstormZkHost(),","));
            jp.setTopworkers(String.valueOf(taskConfigDTO.getWorkerNum()));
            jp.setWorkermem(String.valueOf(taskConfigDTO.getWorkerMem()));

            if(StringUtils.isNotBlank(taskConfigDTO.getSpoutNum()) && !"ExampleSpout:2".equals(taskConfigDTO.getSpoutNum().trim())){
                jp.setSpoutworkers(taskConfigDTO.getSpoutNum());
            }
            if(StringUtils.isNotBlank(taskConfigDTO.getBlotNum()) && !"ExampleFirstBolt:2;ExampleLastBolt:3".equals(taskConfigDTO.getBlotNum().trim())){
                jp.setBlotworkers(taskConfigDTO.getBlotNum());
            }

            String jarFilePath = localJarFilePath;
            String mainClass = taskConfigDTO.getClassPath();
            String params = JSONObject.toJSONString(jp);

            System.setProperty("storm.jar",jarFilePath);
            String paramString= params.replaceAll(" ","");

            LOGGER.error("############JStormTopologyBusiness startTopology paramString="+paramString);
            String pythonString="python "+ EngineContant.JSTORM_HOME +"/bin/jstorm jar "+jarFilePath+" " +mainClass+" "+ Base64.encode(paramString.getBytes()).replaceAll(" ","");
            LOGGER.error("############JStormTopologyBusiness startTopology pythonscrpit="+pythonString);

            Process rt = Runtime.getRuntime().exec(pythonString);


            BufferedReader messageStream = new BufferedReader(new InputStreamReader(rt.getInputStream()));
            BufferedReader errorStream = new BufferedReader(new InputStreamReader(rt.getErrorStream()));

            //打开debug日志流
            printMessageService.submit(new PrintMessageThread(messageStream));

            //打开error日志流
            printMessageService.submit(new PrintErrorThread(errorStream));
        }catch(Exception e){
            LOGGER.error("JStormTopologyBusiness startJob is error",e);
            throw e;
        }
    }

    /**
     * 提交任务拓扑
     * @return
     */
    public static void startTopology(JstormTaskConfigDTO taskConfigDTO) throws Exception{
        if(taskConfigDTO.getTaskCqlId() != null){
            startTopology4CQL(taskConfigDTO);
        }else{
            startTopology4Package(taskConfigDTO);
        }
    }

    /**
     * 提交CQL任务拓扑 暂时没返回结果
     * @return
     */
    public static void startTopology4CQL(JstormTaskConfigDTO taskConfigDTO) throws Exception{
        try{
            //替换掉根
            String jarName= taskConfigDTO.getProjectJarPath().replace(StreamContant.HDFS_PROJECT_PACKAGE_ROOT,"");
            String localJarFilePath = ConfigProperty.getConfigValue(ConfigKeyEnum.LOCAL_PROJECT_ITEM_DIR)+ jarName;
            if(!new File(localJarFilePath).exists()){
                FileUtils.touch(new File(localJarFilePath));
                HdfsClientProxy.downloadFileToLocal(localJarFilePath,taskConfigDTO.getProjectJarPath());
            }

            JstormCqlTaskDTO cqlTaskDTO  = new JstormCqlTaskDTO();
            cqlTaskDTO.setTaskCql(taskConfigDTO.getTaskCql());
            cqlTaskDTO.setTaskName(taskConfigDTO.getTaskName());
            cqlTaskDTO.setZkPort(taskConfigDTO.getJstormZkPort().toString());
            cqlTaskDTO.setZkRoot(taskConfigDTO.getJstormZkRoot());
            cqlTaskDTO.setZkServers(StringUtils.join(taskConfigDTO.getJstormZkHost(),","));
            cqlTaskDTO.setWorkerMemory(String.valueOf(taskConfigDTO.getWorkerMem()));
            cqlTaskDTO.setWorkers(String.valueOf(taskConfigDTO.getWorkerNum()));
            cqlTaskDTO.setProjectJarPath(localJarFilePath);
            JstormCQLSubmitClient.submitCQL(cqlTaskDTO);
        }catch(Exception e){
            LOGGER.error("JStormTopologyBusiness startTopology4CQL is error",e);
            throw e;
        }
    }

    /**
     * 返回正在运行中的任务（不包括kill和inactive的）
     * @param nimbusClient
     * @return
     */
    public static JstormTopologyDTO getTopologyIsRunningWithRetry(NimbusClient nimbusClient,Integer retryTimes, Integer retrySleepSeconds) {
        JstormTopologyDTO entity = null;
        if(nimbusClient == null){
            return entity;
        }
        if(retryTimes == null){
            retryTimes = 3;
        }
        if(retrySleepSeconds == null){
            retrySleepSeconds = 1;
        }
        // 每隔1秒尝试1次，3次都无法连接认为任务已死
        Integer monitorTimes = retryTimes;
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(retrySleepSeconds);
            } catch (InterruptedException e) {
            }
            try {
                List<TopologySummary>  topologies = nimbusClient.getClient().getClusterInfo().get_topologies();
                if(CollectionUtils.isNotEmpty(topologies)){
                    for(TopologySummary topologie:topologies){
                        if(topologie.get_status().toLowerCase().equals(EngineContant.TOP_STATUS_INACTIVE)
                                || topologie.get_status().toLowerCase().equals(EngineContant.TOP_STATUS_KILLED)
                                || topologie.get_status().toLowerCase().equals(EngineContant.TOP_STATUS_STARTING)
                        ){
                            continue;
                        }
                        entity = new JstormTopologyDTO(topologie.get_id(), topologie.get_name(), topologie.get_status(),
                                topologie.get_uptimeSecs(), topologie.get_numTasks(), topologie.get_numWorkers(), topologie.get_errorInfo());
                        return entity;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("JStormTopologyBusiness getTopologyIsRunningWithRetry is error",e);
            }
            if (monitorTimes-- <= 0) {
                return entity;
            }
        }
    }


    private static class PrintMessageThread implements Callable<String> {
        private BufferedReader messageStream;

        public PrintMessageThread(BufferedReader messageStream) {
            this.messageStream = messageStream;
        }

        public String call() throws Exception {
            StringBuilder message=new StringBuilder();
            try {
                String line;
                while((line=messageStream.readLine()) != null){
                    LOGGER.error("debug Info: " + line);
                    message.append(line);
                    message.append(EngineContant.LOG_spliter);
                }
            }catch (Throwable e){
                LOGGER.error("JStormTopologyBusiness PrintMessageThread is complate");
            }finally {
                if(messageStream!=null){
                    try {
                        messageStream.close();
                    }catch (Exception e){
                    }
                }
                messageStream = null;
            }
            return message.toString();
        }
    }

    private static class PrintErrorThread implements Runnable {
        private BufferedReader errorStream;

        public PrintErrorThread(BufferedReader errorStream) {
            this.errorStream = errorStream;
        }

        public void run()  {
            StringBuilder message=new StringBuilder();
            try {
                String line;
                while((line=errorStream.readLine()) != null){
                    LOGGER.error("error Info: " + line);
                    message.append(line);
                    message.append(EngineContant.LOG_spliter);
                }
            }catch (Throwable e){
                LOGGER.error("JStormTopologyBusiness PrintErrorThread is complate");
            }finally {
                if(errorStream!=null){
                    try {
                        errorStream.close();
                    }catch (Exception e){
                    }
                }
                errorStream = null;
            }
        }
    }
}
