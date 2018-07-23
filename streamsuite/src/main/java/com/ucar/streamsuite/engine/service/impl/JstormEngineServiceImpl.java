package com.ucar.streamsuite.engine.service.impl;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.*;

import com.ucar.streamsuite.common.util.*;
import com.ucar.streamsuite.config.po.EngineVersionPO;

import com.ucar.streamsuite.cql.po.CqlPO;
import com.ucar.streamsuite.dao.mysql.*;
import com.ucar.streamsuite.engine.business.*;
import com.ucar.streamsuite.engine.constants.EngineContant;
import com.ucar.streamsuite.engine.constants.YarnZkContant;
import com.ucar.streamsuite.engine.dto.*;
import com.ucar.streamsuite.engine.po.JstormProcessPO;
import com.ucar.streamsuite.moniter.business.impl.MetricReportContainer;
import com.ucar.streamsuite.moniter.service.impl.JstormMetricCollectImpl;
import com.ucar.streamsuite.task.dto.TaskDTO;
import com.ucar.streamsuite.task.dto.TaskStartTimeLineDTO;
import com.ucar.streamsuite.task.po.TaskArchivePO;
import com.ucar.streamsuite.task.po.TaskArchiveVersionPO;
import com.ucar.streamsuite.task.po.TaskPO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;

import org.apache.hadoop.yarn.api.records.*;

import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Description: jstorm任务服务类实现类
 * Created on 2018/1/30 上午10:59
 *
 *
 */
@Service
public class JstormEngineServiceImpl  {

    private static final Logger LOGGER = LoggerFactory.getLogger(JstormEngineServiceImpl.class);

    @Autowired
    private TaskDao taskDao;
    @Autowired
    private CqlDao cqlDao;
    @Autowired
    private JstormProcessDao jstormProcessDao;
    @Autowired
    private TaskArchiveDao taskArchiveDao;
    @Autowired
    private TaskArchiveVersionDao taskArchiveVersionDao;
    @Autowired
    private EngineVersionDao engineVersionDao;

    private static JstormEngineServiceImpl jstormEngineService;

    //初始化静态参数
    @PostConstruct
    public void init() {
        jstormEngineService = this;
    }

    public static Map<TaskPO,JstormProcessPO> listAllTaskToProcess() {
        Map<TaskPO,JstormProcessPO> taskToProcessMap = Maps.newHashMap();
        List<TaskPO> taskPOs = jstormEngineService.taskDao.listAll(EngineTypeEnum.JSTORM.getValue());
        if(CollectionUtils.isEmpty(taskPOs)){
            return taskToProcessMap;
        }

        Set<Integer> processIds = Sets.newHashSet();
        for(TaskPO taskPO: taskPOs){
            if(taskPO.getProcessId() == null || taskPO.getProcessId()<= 0){
                continue;
            }
            processIds.add(taskPO.getProcessId());
        }
        if(processIds.isEmpty()){
            return taskToProcessMap;
        }

        Map<String,Object> params = Maps.newHashMap();
        params.put("ids", processIds);
        List<JstormProcessPO> jstormProcessPOs = jstormEngineService.jstormProcessDao.getByIds(params);
        if(CollectionUtils.isEmpty(jstormProcessPOs)){
            return taskToProcessMap;
        }

        for(TaskPO taskPO: taskPOs){
            for(JstormProcessPO jstormProcessPO: jstormProcessPOs){
                if(taskPO.getProcessId().equals(jstormProcessPO.getId()) && StringUtils.isNotBlank(jstormProcessPO.getYarnAppId())){
                    taskToProcessMap.put(taskPO,jstormProcessPO);
                }
            }
        }
        return taskToProcessMap;
    }

    public static String validateAndBuildTaskConfig(TaskDTO taskDTO) throws Exception {

        JstormTaskConfigDTO jstormTaskConfigDTO = new JstormTaskConfigDTO();
        jstormTaskConfigDTO.setTaskName(taskDTO.getTaskName());

        if(taskDTO.getIsCql() == YesOrNoEnum.NO.getValue()){
            if(StringUtils.isBlank(taskDTO.getClassPath())){
                throw new Exception("任务的类路径不能为空！");
            }
            if(StringUtils.isBlank(taskDTO.getArchiveId())){
                throw new Exception("请选择任务运行文件！");
            }
            TaskArchivePO taskArchivePO = jstormEngineService.taskArchiveDao.getTaskArchiveById(Integer.valueOf(taskDTO.getArchiveId()));
            if(taskArchivePO == null ){
                throw new Exception("任务运行文件不存在，或已经被删除！");
            }
            if(taskDTO.getArchiveVersionId()== null){
                throw new Exception("请选择任务运行文件版本！");
            }
            TaskArchiveVersionPO taskArchiveVersionPO = jstormEngineService.taskArchiveVersionDao.getTaskArchiveVersionById(Integer.valueOf(taskDTO.getArchiveVersionId()));
            if(taskArchiveVersionPO == null ){
                throw new Exception("任务运行文件包不存在，或已经被删除！");
            }
            jstormTaskConfigDTO.setProjectJarPath(taskArchiveVersionPO.getTaskArchiveVersionUrl());
            jstormTaskConfigDTO.setClassPath(taskDTO.getClassPath());
            jstormTaskConfigDTO.setBlotNum(taskDTO.getBlotNum());
            jstormTaskConfigDTO.setSpoutNum(taskDTO.getSpoutNum());
        }else{
            if(StringUtils.isBlank(taskDTO.getTaskCqlId())|| Integer.valueOf(taskDTO.getTaskCqlId())<=0){
                throw new Exception("请选择CQL脚本！");
            }
            CqlPO cqlPO = jstormEngineService.cqlDao.getCqlById( Integer.valueOf(taskDTO.getTaskCqlId()));
            if(cqlPO == null){
                throw new Exception("选择的CQL脚本不存在或已经被删除！");
            }
            jstormTaskConfigDTO.setTaskCqlId(cqlPO.getId());
        }

        //Jstorm引擎和AM引擎
        if(StringUtils.isBlank(taskDTO.getYarnAmEngineVersionId()) || StringUtils.isBlank(taskDTO.getJstormEngineVersionId()) ){
            throw new Exception("请选择 Jstorm 和 Jstorm_AM 引擎版本包 ！");
        }
        EngineVersionPO engineVersionAM = jstormEngineService.engineVersionDao.getEngineVersionById(Integer.valueOf(taskDTO.getYarnAmEngineVersionId()));
        if(engineVersionAM == null ){
            throw new Exception("Jstorm_AM 引擎版本包不存在，或已经被删除！");
        }
        jstormTaskConfigDTO.setYarnAmJarId(engineVersionAM.getId());
        jstormTaskConfigDTO.setYarnAmJarPath(engineVersionAM.getVersionUrl());
        EngineVersionPO engineVersionJstorm =  jstormEngineService.engineVersionDao.getEngineVersionById(Integer.valueOf(taskDTO.getJstormEngineVersionId()));
        if(engineVersionJstorm == null ){
            throw new Exception("Jstorm 引擎版本包不存在，或已经被删除！");
        }
        jstormTaskConfigDTO.setJstormJarId(engineVersionJstorm.getId());
        jstormTaskConfigDTO.setJstormJarPath(engineVersionJstorm.getVersionUrl());

        //ZK信息
        String zkHosts = ConfigProperty.getConfigValue(ConfigKeyEnum.ZK_HOST);
        String zkPort = ConfigProperty.getConfigValue(ConfigKeyEnum.ZK_PORT);
        if(StringUtils.isBlank(zkHosts)){
            throw new Exception("zk 主机地址没有配置，请联系管理员进行配置！");
        }
        if(StringUtils.isBlank(zkPort)){
            throw new Exception("zk 端口没有配置，请联系管理员进行配置！");
        }
        if(taskDTO.getWorkerNum()==null||Integer.valueOf(taskDTO.getWorkerNum())<=0){
            throw new Exception("jstorm worker 个数不能为空，并且只能为正整数！");
        }
        if(taskDTO.getWorkerMem()==null||Integer.valueOf(taskDTO.getWorkerMem())<=0){
            throw new Exception("jstorm worker 内存大小不能为空，并且只能为正整数！");
        }

        jstormTaskConfigDTO.setJstormZkHost(Lists.newArrayList(StringUtils.split(zkHosts,",")));
        jstormTaskConfigDTO.setJstormZkPort(Integer.valueOf(zkPort));
        jstormTaskConfigDTO.setJstormZkRoot(EngineContant.YARN_APP_PREFIX + jstormTaskConfigDTO.getTaskName());
        jstormTaskConfigDTO.setWorkerMem(Long.valueOf(taskDTO.getWorkerMem()));
        jstormTaskConfigDTO.setWorkerNum(Integer.valueOf(taskDTO.getWorkerNum()));
        return JSONObject.toJSONString(jstormTaskConfigDTO);
    }

    public static void beginTask(Integer taskId,String taskConfig) throws Exception{
        if(taskId == null){
            throw new Exception("任务提交失败，参数异常，taskId为空！");
        }
        if(StringUtils.isBlank(taskConfig)){
            throw new Exception("任务提交失败，参数异常，任务配置信息为空！");
        }
        JstormTaskConfigDTO jstormTaskConfigDTO = JSONObject.parseObject(taskConfig,JstormTaskConfigDTO.class);
        jstormTaskConfigDTO.setTaskId(taskId);
        if(StringUtils.isBlank(jstormTaskConfigDTO.getTaskName())){
            throw new Exception("任务提交失败，参数异常，任务名为空！");
        }
        if(jstormTaskConfigDTO.getWorkerNum() == null || jstormTaskConfigDTO.getWorkerNum()<=0){
            throw new Exception("任务提交失败，参数异常，worker 数必须为正整数！");
        }
        if(jstormTaskConfigDTO.getWorkerMem() == null || jstormTaskConfigDTO.getWorkerMem()<=0){
            throw new Exception("任务提交失败，参数异常，worker 内存必须为正整数！");
        }
        if(StringUtils.isBlank(jstormTaskConfigDTO.getJstormJarPath())){
            throw new Exception("任务提交失败，参数异常，jstorm jar包位置为空！");
        }
        if(StringUtils.isBlank(jstormTaskConfigDTO.getYarnAmJarPath())){
            throw new Exception("任务提交失败，参数异常，yarn am jar包位置为空！");
        }
        if(CollectionUtils.isEmpty(jstormTaskConfigDTO.getJstormZkHost())){
            throw new Exception("任务提交失败，参数异常，jstorm 集群的 zk host为空！");
        }
        if(jstormTaskConfigDTO.getJstormZkPort() == null ||  jstormTaskConfigDTO.getJstormZkPort()<=0 ){
            throw new Exception("任务提交失败，参数异常，jstorm 集群的 zk port为空！");
        }
        EngineContant.JSTORM_HOME = ConfigProperty.getConfigValue(ConfigKeyEnum.JSTORM_HOME);
        if(StringUtils.isBlank(EngineContant.JSTORM_HOME)){
            throw new Exception("任务提交失败，参数异常，系统参数 JSTORM_HOME 未进行配置！");
        }
        if(StringUtils.isBlank(ConfigProperty.getConfigValue(ConfigKeyEnum.LOCAL_PROJECT_ITEM_DIR))){
            throw new Exception("任务提交失败，参数异常，系统参数 LOCAL_PROJECT_ITEM_DIR 未进行配置！");
        }
        if(StringUtils.isBlank(ConfigProperty.getConfigValue(ConfigKeyEnum.JSTORM_TASK_LOG_PREFIX))){
            throw new Exception("任务提交失败，参数异常，系统参数 JSTORM_TASK_LOG_PREFIX 未进行配置！");
        }

        // 这个地方判断，提交时，在去获得一次最新的脚本
        if(jstormTaskConfigDTO.getTaskCqlId()!=null){
            CqlPO cqlPO = jstormEngineService.cqlDao.getCqlById(jstormTaskConfigDTO.getTaskCqlId());
            if(cqlPO == null){
                throw new Exception("选择的CQL脚本不存在或已经被删除！");
            }
            jstormTaskConfigDTO.setTaskCql(cqlPO.getCqlText());

            String CQL_ENGINE_PROJECT_NAME = ConfigProperty.getConfigValue(ConfigKeyEnum.CQL_ENGINE_PROJECT_NAME);
            if(StringUtils.isBlank(CQL_ENGINE_PROJECT_NAME)){
                throw new Exception("任务提交失败，参数异常，系统参数 CQL_ENGINE_PROJECT_NAME 未进行配置！");
            }
            String cqlEngineUrl = jstormEngineService.taskArchiveVersionDao.getCqlEngineUrl(CQL_ENGINE_PROJECT_NAME);
            if(StringUtils.isBlank(cqlEngineUrl)){
                throw new Exception("任务提交失败，未初始化CQL任务引擎！");
            }
            jstormTaskConfigDTO.setProjectJarPath(cqlEngineUrl);
        }
        EngineBusiness.pendingTask(jstormTaskConfigDTO.getTaskName(),jstormTaskConfigDTO);
    }

    public static void stopTask(Integer taskId, String taskName, Integer processId) throws Exception {
        if(taskId == null){
            throw new Exception("任务停止失败，参数异常，taskId为空！");
        }
        if(StringUtils.isBlank(taskName)){
            throw new Exception("任务提交失败，参数异常，任务名为空！");
        }
        if(processId == null){
            throw new Exception("任务停止失败，参数异常，processId为空！");
        }
        JstormProcessPO jstormProcessPO = jstormEngineService.jstormProcessDao.getById(processId);
        if(jstormProcessPO == null){
            throw new Exception("任务停止失败，任务的执行信息不存在或者已经被删除！");
        }
        JstormTaskConfigDTO jstormTaskConfigDTO = JSONObject.parseObject(jstormProcessPO.getTaskConfig(), JstormTaskConfigDTO.class);

        String topId = jstormProcessPO.getTopId();
        String appId = jstormProcessPO.getYarnAppId();
        String applicationPath = YarnZkRegistryBusiness.PathBuilder.applicationPath(jstormTaskConfigDTO.getTaskName(),jstormProcessPO.getYarnAppId());
        ServiceRecord serviceRecord  = YarnZkRegistryBusiness.resolve(applicationPath);
        if(serviceRecord != null){
            // 设置为true
            serviceRecord.set("killed","true");
            YarnZkRegistryBusiness.bind(applicationPath,serviceRecord);
        }

        ApplicationReport report = YarnClientProxy.getApplicationReportByAppId(appId);
        if(report != null){
            YarnApplicationState state = report.getYarnApplicationState();

            LOGGER.error("JstormProcessServiceImpl stopTask report YarnApplicationState = " + state.toString() + " appId=" + appId);

            if (!(YarnApplicationState.FINISHED == state || YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state)) {
                try{
                    YarnClientProxy.killApplicationByAppId(appId);
                    LOGGER.error("JstormProcessServiceImpl stopTask killApplicationByAppId is ok appId=" + appId);
                }catch (Exception e){
                    // kill失败时进行还原处理
                    if(serviceRecord != null){
                        serviceRecord.set("killed","false");
                        YarnZkRegistryBusiness.bind(applicationPath,serviceRecord);
                    }
                    LOGGER.error("JstormProcessServiceImpl stopTask killApplicationByAppId is error appId=" + appId,e);
                    throw e;
                }
            }
        }else{
            LOGGER.error("JstormProcessServiceImpl stopTask report is null appId=" + appId);
        }

        TaskPO taskPO = new TaskPO();
        taskPO.setTaskStatus(TaskStatusEnum.STOP.getValue());
        taskPO.setTaskStopTime(new Date());
        taskPO.setId(jstormProcessPO.getTaskId());
        jstormEngineService.taskDao.update4Stop(taskPO);

        //清理资源
        JStormOnYarnBusiness.clearZkInfo(jstormTaskConfigDTO.getJstormZkHost(), jstormTaskConfigDTO.getJstormZkPort(), jstormTaskConfigDTO.getTaskName());
        JstormMetricCollectImpl.workerErrorHistoryCache.remove(topId);
        MetricReportContainer.removeReports(topId);
        JstormEngineCheckPointImpl.recoveryFail.remove(jstormProcessPO.getTaskId());
    }

    /**
     * 任务提交
     * @param jstormTaskConfigDTO
     * @param submitTypeEnum
     * @return
     */
    public static boolean taskSubmit(JstormTaskConfigDTO jstormTaskConfigDTO , TaskSubmitTypeEnum submitTypeEnum) {
        NimbusClient nimbusClient = null;
        boolean submitOk = false;
        try {
            jstormTaskConfigDTO.setSubmitType(submitTypeEnum.getValue());

            JstormYarnAppSubmitDTO  jstormYarnAppSubmitDTO = new JstormYarnAppSubmitDTO();

            TaskStartTimeLineDTO taskStartTimeLineDTO = new TaskStartTimeLineDTO(jstormTaskConfigDTO.getTaskName(),"准备提交任务，正在进行预处理");
            EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO);

            Integer processId = topologySubmit4CreateYarnApp(jstormTaskConfigDTO, taskStartTimeLineDTO, jstormYarnAppSubmitDTO);
            if (processId!=null) {
                nimbusClient = topologySubmit4ValidateCluster(jstormTaskConfigDTO, taskStartTimeLineDTO, jstormYarnAppSubmitDTO);
                if (nimbusClient != null) {
                    submitOk = topologySubmit4SubmitTopology(jstormTaskConfigDTO, taskStartTimeLineDTO, jstormYarnAppSubmitDTO, nimbusClient,processId);
                }
            }
        } catch (Exception e) {
            LOGGER.error("topologySubmit is error",e);
        } finally {
            if(nimbusClient!=null){
                nimbusClient.close();
            }
        }
        return submitOk;
    }

    /**
     * 拓扑提交步骤1，建立appOnYarn
     * @param jstormTaskConfigDTO
     * @param taskStartTimeLineDTO
     * @param jstormYarnAppSubmitDTO
     * @return
     */
    private static Integer topologySubmit4CreateYarnApp(JstormTaskConfigDTO jstormTaskConfigDTO, TaskStartTimeLineDTO taskStartTimeLineDTO, JstormYarnAppSubmitDTO jstormYarnAppSubmitDTO) {
        Integer processId = null;
        ApplicationId applicationId=null;
        try {
            //提交历史
            JstormProcessPO jstormProcessPO= new JstormProcessPO();
            jstormProcessPO.setTaskId(jstormTaskConfigDTO.getTaskId());
            jstormProcessPO.setStartTime(new Date());
            jstormProcessPO.setSubmitType(jstormTaskConfigDTO.getSubmitType());
            jstormProcessPO.setTaskConfig(JSONObject.toJSONString(jstormTaskConfigDTO));
            jstormEngineService.jstormProcessDao.insert(jstormProcessPO);
            processId = jstormProcessPO.getId();

            LOGGER.error("topolgoySubmit submitJstormOnYarnApp begin JstormTaskConfigDTO = " + JSONObject.toJSONString(jstormTaskConfigDTO));

            String NIMBUS_CONTAINER_DEFAULT_MEM = ConfigProperty.getConfigValue(ConfigKeyEnum.JSTORM_NIMBUS_MEM);

            Integer supervisorContainerMemery = 0;  // supervisor container内存数 单位MB
            Integer supervisorContainerNum = 0; // supervisor container的个数
            if(jstormTaskConfigDTO.getWorkerNum() < EngineContant.SUPERVISOR_DEFAULT_PORT){
                supervisorContainerNum = 1;
                supervisorContainerMemery =  EngineContant.SUPERVISOR_OWNER_MEM  + jstormTaskConfigDTO.getWorkerNum() * 1024;
            }else{
                if(jstormTaskConfigDTO.getWorkerNum() % EngineContant.SUPERVISOR_DEFAULT_PORT == 0){
                    supervisorContainerNum = jstormTaskConfigDTO.getWorkerNum() / EngineContant.SUPERVISOR_DEFAULT_PORT;
                }else{
                    supervisorContainerNum =jstormTaskConfigDTO.getWorkerNum() / EngineContant.SUPERVISOR_DEFAULT_PORT + 1;
                }
                supervisorContainerMemery = EngineContant.SUPERVISOR_OWNER_MEM  + (EngineContant.SUPERVISOR_DEFAULT_PORT * 1024);
            }

            //JstormTaskConfigDTO 转换为 JstormYarnAppSubmitDTO 准备提交AM的参数信息
            jstormYarnAppSubmitDTO.setJstormJarPath(jstormTaskConfigDTO.getJstormJarPath());
            jstormYarnAppSubmitDTO.setNimbusMemery(StringUtils.trimToEmpty(NIMBUS_CONTAINER_DEFAULT_MEM).equals("")?EngineContant.NIMBUS_OWNER_MEM: Integer.valueOf(NIMBUS_CONTAINER_DEFAULT_MEM));
            jstormYarnAppSubmitDTO.setNimbusNum(1);
            jstormYarnAppSubmitDTO.setSupervisorMemery(supervisorContainerMemery.intValue());
            jstormYarnAppSubmitDTO.setSupervisorNum(supervisorContainerNum);
            jstormYarnAppSubmitDTO.setSupervisorportNum(EngineContant.SUPERVISOR_DEFAULT_PORT);
            jstormYarnAppSubmitDTO.setYarnAmJarPath(jstormTaskConfigDTO.getYarnAmJarPath());
            jstormYarnAppSubmitDTO.setJstormZkHost(StringUtils.join(jstormTaskConfigDTO.getJstormZkHost(),","));
            jstormYarnAppSubmitDTO.setJstormZkRoot(jstormTaskConfigDTO.getJstormZkRoot());
            jstormYarnAppSubmitDTO.setJstormZkPort(jstormTaskConfigDTO.getJstormZkPort());
            jstormYarnAppSubmitDTO.setClusterName(jstormTaskConfigDTO.getTaskName());

            EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_OK,"正在向yarn环境提交APP，启动AM"));

            applicationId = JStormOnYarnBusiness.submitYarnApp(jstormYarnAppSubmitDTO);
            jstormYarnAppSubmitDTO.setYarnAppId(applicationId.toString());

            LOGGER.error("topolgoySubmit submitJstormOnYarnApp is OK JstormTaskConfigDTO = " + JSONObject.toJSONString(jstormTaskConfigDTO));

            // app成功提交后更新YarnAppId
            jstormProcessPO.setYarnAppId(applicationId.toString());
            jstormEngineService.jstormProcessDao.update(jstormProcessPO);

        } catch (Throwable e) {
            LOGGER.error("topolgoySubmit submitJstormOnYarnApp is error",e);
            try{
                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_FAILD,"yarn环境提交APP时失败，正在进行回滚"));
                if(jstormYarnAppSubmitDTO.getYarnAppId() != null){
                    JStormOnYarnBusiness.killApplication(jstormTaskConfigDTO.getJstormZkHost(), jstormTaskConfigDTO.getJstormZkPort(), jstormYarnAppSubmitDTO.getYarnAppId(),jstormTaskConfigDTO.getTaskName());
                }
                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToEnd(EngineContant.TIMELINE_ITEM_OK, "任务开始失败！" + e.getMessage(), EngineContant.TIMELINE_FAILD));
            }catch(Exception e1){
                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToEnd(EngineContant.TIMELINE_ITEM_FAILD, "任务开始失败！" + e.getMessage(), EngineContant.TIMELINE_FAILD));
            }
            return null;
        }
        return processId;
    }

    /**
     * 拓扑提交步骤2，检查集群
     * @param jstormTaskConfigDTO
     * @param taskStartTimeLineDTO
     * @param jstormYarnAppSubmitDTO
     * @return
     */
    private static NimbusClient topologySubmit4ValidateCluster(JstormTaskConfigDTO jstormTaskConfigDTO, TaskStartTimeLineDTO taskStartTimeLineDTO, JstormYarnAppSubmitDTO jstormYarnAppSubmitDTO) {
        NimbusClient nimbusClient;
        try {
            EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_OK,"yarn环境启动AM成功，正在验证Jstorm集群可用性"));
            LOGGER.error("topolgoySubmit validateJstormCluster begin JstormTaskConfigDTO = " + JSONObject.toJSONString(jstormTaskConfigDTO));

            nimbusClient = JStormClusterBusiness.getNimBusClientWithRetry(jstormTaskConfigDTO.getJstormZkHost(),
                    Integer.valueOf(jstormTaskConfigDTO.getJstormZkPort()),jstormYarnAppSubmitDTO.getJstormZkRoot(),10,3);

            if(nimbusClient == null){
                throw new Exception("yarn 环境中的Jstorm集群启动失败!");
            }

            LOGGER.error("topolgoySubmit validateJstormCluster is OK JstormTaskConfigDTO = " + JSONObject.toJSONString(jstormTaskConfigDTO));
        } catch (Throwable e) {
            LOGGER.error("topolgoySubmit validateJstormCluster is error",e);
            try{
                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_FAILD,"Jstorm集群启动出现异常，正在进行回滚"));
                JStormOnYarnBusiness.killApplication(jstormTaskConfigDTO.getJstormZkHost(), jstormTaskConfigDTO.getJstormZkPort(), jstormYarnAppSubmitDTO.getYarnAppId(),jstormTaskConfigDTO.getTaskName());
                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToEnd(EngineContant.TIMELINE_ITEM_OK, "任务开始失败！", EngineContant.TIMELINE_FAILD));
            }catch(Exception e1){
                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToEnd(EngineContant.TIMELINE_ITEM_FAILD, "任务开始失败！", EngineContant.TIMELINE_FAILD));
            }
            return null;
        }
        return nimbusClient;
    }

    /**
     * 拓扑提交步骤3，提交拓扑到集群
     * @param jstormTaskConfigDTO
     * @param taskStartTimeLineDTO
     * @param jstormYarnAppSubmitDTO
     * @return
     */
    private static boolean topologySubmit4SubmitTopology(JstormTaskConfigDTO jstormTaskConfigDTO, TaskStartTimeLineDTO taskStartTimeLineDTO
            , JstormYarnAppSubmitDTO jstormYarnAppSubmitDTO, NimbusClient nimbusClient, Integer processId) {
        try {
            Integer taskId = jstormTaskConfigDTO.getTaskId();

            EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext("green","Jstorm集群可用性验证完成，正在向jstorm集群提交拓扑"));
            LOGGER.error("topolgoySubmit startTopology begin JstormTaskConfigDTO = " + JSONObject.toJSONString(jstormTaskConfigDTO));

            //重新计算内存 页面上填写的是M
            jstormTaskConfigDTO.setWorkerMem(jstormTaskConfigDTO.getWorkerMem()*1024*1024);
            JStormTopologyBusiness.startTopology(jstormTaskConfigDTO);

            // 如果启动失败，没有任务则回滚：等待1分半 （这个是个估计值。需要做成配置）
            Integer retryTimes = 50;
            Integer retrySleepSeconds = 3;
            String timeOut = ConfigProperty.getConfigValue(ConfigKeyEnum.JSTORM_TOP_START_TIMEOUT);
            if(!StringUtils.trimToEmpty(timeOut).equals("")){
                retryTimes = Integer.valueOf(timeOut) / retrySleepSeconds + (Integer.valueOf(timeOut) % retrySleepSeconds > 0 ? 1: 0) ;
            }

            JstormTopologyDTO jstormTopologyDTO = JStormTopologyBusiness.getTopologyIsRunningWithRetry(nimbusClient,retryTimes,retrySleepSeconds);
            if(jstormTopologyDTO == null){
                 throw new Exception("topolgoySubmit failed have not active or starting top");
            }

            // 更新topID 和 YarnAppMetadata
            JstormProcessPO updateJstormProcessPO = new JstormProcessPO();
            updateJstormProcessPO.setId(processId);
            updateJstormProcessPO.setTopId(jstormTopologyDTO.getTopId());
            Set<AppContainerIdDTO> inUseZkContainers = JStormOnYarnBusiness.refreshAndGetZkAppContainer(jstormYarnAppSubmitDTO.getYarnAppId(),jstormTaskConfigDTO.getTaskName());
            updateJstormProcessPO.setYarnAppMetadata(JSONObject.toJSONString(inUseZkContainers));
            jstormEngineService.jstormProcessDao.update(updateJstormProcessPO);

            // 最后再查一遍，避免数据已经被删除
            TaskPO taskPO = jstormEngineService.taskDao.getById(taskId);
            if(taskPO == null){
                throw new Exception("task is deleted !!!");
            }

            // 更新为提交成功
            jstormEngineService.jstormProcessDao.updateSubmitOk(processId);

            taskPO.setTaskStatus(TaskStatusEnum.RUNNING.getValue());
            taskPO.setTaskStartTime(new Date());
            taskPO.setProcessId(processId);
            taskPO.setId(taskId);
            jstormEngineService.taskDao.update4Start(taskPO);

            EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToEnd(EngineContant.TIMELINE_ITEM_OK,"任务开始成功！", EngineContant.TIMELINE_OK));
            LOGGER.error("topolgoySubmit startTopology is ok topId = " + jstormTopologyDTO.getTopId() + " taskId=" + taskId + " JstormTaskConfigDTO = " + JSONObject.toJSONString(jstormTaskConfigDTO));

            //启动成功先保存一次
            JstormEngineServiceImpl.processTaskSavepoint(jstormYarnAppSubmitDTO.getYarnAppId(),jstormTopologyDTO.getTopId(),JstormTopologySavePointEnum.COMMON.getValue(),processId,jstormTaskConfigDTO);
            return true;
        } catch (Throwable e) {
            LOGGER.error("topolgoySubmit startTopology is error",e);
            try{
                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_FAILD,"向jstorm集群提交拓扑时出现异常，正在进行回滚"));
                JStormOnYarnBusiness.killApplication(jstormTaskConfigDTO.getJstormZkHost(), jstormTaskConfigDTO.getJstormZkPort(), jstormYarnAppSubmitDTO.getYarnAppId(),jstormTaskConfigDTO.getTaskName());
                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToEnd(EngineContant.TIMELINE_ITEM_OK, "任务开始失败！", EngineContant.TIMELINE_FAILD));
            }catch(Exception e1){
                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToEnd(EngineContant.TIMELINE_ITEM_FAILD, "任务开始失败！", EngineContant.TIMELINE_FAILD));
            }
            return false;
        }
    }

    public static void fillTaskConfigToDto(TaskDTO taskDTO, String taskConfig,boolean withShow) {
        JstormTaskConfigDTO jstormTaskConfigDTO = JSONObject.parseObject(taskConfig, JstormTaskConfigDTO.class);
        taskDTO.setZkHosts(StringUtils.join(jstormTaskConfigDTO.getJstormZkHost(),","));
        taskDTO.setZkPort(jstormTaskConfigDTO.getJstormZkPort());
        taskDTO.setWorkerMem(jstormTaskConfigDTO.getWorkerMem().toString());
        taskDTO.setWorkerNum(jstormTaskConfigDTO.getWorkerNum().toString());
        taskDTO.setJstormEngineVersionId(jstormTaskConfigDTO.getJstormJarId().toString());
        taskDTO.setYarnAmEngineVersionId(jstormTaskConfigDTO.getYarnAmJarId().toString());
        taskDTO.setClassPath(jstormTaskConfigDTO.getClassPath());
        taskDTO.setTaskCql(jstormTaskConfigDTO.getTaskCql());
        taskDTO.setTaskCqlId(jstormTaskConfigDTO.getTaskCqlId()==null?"":jstormTaskConfigDTO.getTaskCqlId().toString());
        taskDTO.setBlotNum(StringUtils.trimToEmpty(jstormTaskConfigDTO.getBlotNum()));
        taskDTO.setSpoutNum(StringUtils.trimToEmpty(jstormTaskConfigDTO.getSpoutNum()));

        //填充详细信息
        if(withShow){
            taskDTO.setJstormEngineVersionShow(jstormTaskConfigDTO.getJstormJarPath());
            taskDTO.setYarnAmEngineVersionShow(jstormTaskConfigDTO.getYarnAmJarPath());
            List<String> zkAddress = Lists.newArrayList();
            for(String jstormZkHost: jstormTaskConfigDTO.getJstormZkHost()){
                zkAddress.add(jstormZkHost + ":" + jstormTaskConfigDTO.getJstormZkPort());
            }
            taskDTO.setZkAddressShow(StringUtils.join(zkAddress,","));

            if(StringUtils.isNotBlank(taskDTO.getTaskCqlId())){
                CqlPO cqlPO = jstormEngineService.cqlDao.getCqlById(Integer.valueOf(taskDTO.getTaskCqlId()));
                if(cqlPO != null ){
                    String remark = StringUtils.isNotBlank(cqlPO.getCqlRemark())?" [" + cqlPO.getCqlRemark() +"]":"";
                    taskDTO.setTaskCqlShow(cqlPO.getCqlName() + remark);
                }
            }
        }
    }

    public static void processTaskSavepoint(String yarnAppId, String topId, Integer savepointType,Integer processId,JstormTaskConfigDTO taskConfigDTO) {
        try{
            if(JstormTopologySavePointEnum.COMMON.getValue() == savepointType){

                ApplicationReport applicationReport = YarnClientProxy.getApplicationReportByAppId(yarnAppId);
                if(applicationReport == null){
                    updateSavePointType(JstormTopologySavePointEnum.APP_ERROR.getValue(), processId);
                    return;
                }

                List<ContainerReport> containerReports = YarnClientProxy.getContainersByAppAttemptId(applicationReport.getCurrentApplicationAttemptId());
                if(CollectionUtils.isEmpty(containerReports)){
                    updateSavePointType(JstormTopologySavePointEnum.APP_ERROR.getValue(), processId);
                    return;
                }

                NimbusClient nimbusClient = JStormClusterBusiness.getNimBusClientWithRetry(taskConfigDTO.getJstormZkHost(),taskConfigDTO.getJstormZkPort(),taskConfigDTO.getJstormZkRoot(),2,2);
                if(nimbusClient==null){
                    updateSavePointType(JstormTopologySavePointEnum.NIMBUS_ERROR.getValue(), processId);
                    return;
                }

                ClusterSummary clusterSummary = null;
                try{
                    clusterSummary = nimbusClient.getClient().getClusterInfo();
                }catch (Exception e){
                    updateSavePointType(JstormTopologySavePointEnum.NIMBUS_ERROR.getValue(), processId);
                    return;
                }

                TopologyInfo topologyInfo = JStormClusterBusiness.getTopologyInfoWithRetry(nimbusClient,topId);
                if(topologyInfo==null){
                    updateSavePointType(JstormTopologySavePointEnum.TOP_ERROR.getValue(), processId);
                    return;
                }

                JstormTaskDetailDTO jstormTaskDetailDTO = new JstormTaskDetailDTO();

                JstormClusterDTO jstormClusterDTO = new JstormClusterDTO();
                jstormTaskDetailDTO.setJstormClusterDTO(jstormClusterDTO);

                jstormClusterDTO.setClusterName(taskConfigDTO.getTaskName());
                jstormClusterDTO.setAppId(applicationReport.getApplicationId().toString());
                jstormClusterDTO.setAppAmHost(applicationReport.getHost());
                jstormClusterDTO.setAppAmPort(String.valueOf(applicationReport.getRpcPort()));
                jstormClusterDTO.setAppStartTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(applicationReport.getStartTime())));
                jstormClusterDTO.setAppState(applicationReport.getYarnApplicationState().toString());
                jstormClusterDTO.setTopId(topId);
                jstormClusterDTO.setClusterZkHost(StringUtils.join(taskConfigDTO.getJstormZkHost(),",") + "(" +taskConfigDTO.getJstormZkPort().toString() +")");
                jstormClusterDTO.setClusterZkPort(taskConfigDTO.getJstormZkPort().toString());
                jstormClusterDTO.setClusterZkRoot(taskConfigDTO.getJstormZkRoot());
                jstormClusterDTO.setClusterSlotsTotal(String.valueOf(clusterSummary.get_nimbus().get_totalPortNum()));
                jstormClusterDTO.setClusterSlotsUsed(String.valueOf(clusterSummary.get_nimbus().get_usedPortNum()));
                jstormClusterDTO.setClusterSlots(String.valueOf(clusterSummary.get_nimbus().get_usedPortNum()) + "/" + String.valueOf(clusterSummary.get_nimbus().get_totalPortNum()));

                //拿每个container的信息
                for(ContainerReport containerReport:containerReports){

                    String containerPath = YarnZkRegistryBusiness.PathBuilder.containerPath(taskConfigDTO.getTaskName(), jstormClusterDTO.getAppId(), ConverterUtils.toString(containerReport.getContainerId()));
                    if(!YarnZkRegistryBusiness.exists(containerPath)){
                        continue;
                    }

                    ServiceRecord serviceRecord = YarnZkRegistryBusiness.resolve(containerPath);
                    if(serviceRecord == null){
                        continue;
                    }

                    String jstorm_type = serviceRecord.get(YarnZkContant.ZK_CONTAINER_TYPE);
                    // 如果是 nimbus
                    if(StringUtils.isNotBlank(jstorm_type) && jstorm_type.equals(YarnZkContant.ZK_CONTAINER_TYPE_NIMBUS)){
                        JstormClusterNimbusDTO nimbusInfo = new JstormClusterNimbusDTO();
                        jstormClusterDTO.addNimbusInfo(nimbusInfo);

                        nimbusInfo.setHost(NetUtil.getHostFromDomain(serviceRecord.get(YarnZkContant.ZK_CONTAINER_JSTORM_HOST)) + ":" + serviceRecord.get(YarnZkContant.ZK_CONTAINER_NIMBUS_PORT));
                        Integer sec = Integer.parseInt(Long.toString((System.currentTimeMillis() - containerReport.getCreationTime())/1000));

                        nimbusInfo.setUptime(DateUtil.prettyUptime(sec));
                        nimbusInfo.setContainerId(ConverterUtils.toString(containerReport.getContainerId()));
                        nimbusInfo.setContainerStats(containerReport.getContainerState().toString());
                    }

                    // 如果是 supervisor
                    if(StringUtils.isNotBlank(jstorm_type) && jstorm_type.equals(YarnZkContant.ZK_CONTAINER_TYPE_SUPERVISOR)){
                        JstormClusterSupervisoDTO supervisorInfo = new JstormClusterSupervisoDTO();
                        jstormClusterDTO.addSupervisorInfo(supervisorInfo);

                        supervisorInfo.setHost(NetUtil.getHostFromDomain(serviceRecord.get(YarnZkContant.ZK_CONTAINER_JSTORM_HOST)));

                        Integer sec = Integer.parseInt(Long.toString((System.currentTimeMillis() - containerReport.getCreationTime())/1000));
                        supervisorInfo.setUptime(DateUtil.prettyUptime(sec));
                        supervisorInfo.setPortsList(serviceRecord.get(YarnZkContant.ZK_CONTAINER_SUPERVISOR_PORT_LIST));
                        supervisorInfo.setContainerId(ConverterUtils.toString(containerReport.getContainerId()));
                        supervisorInfo.setContainerStats(containerReport.getContainerState().toString());
                    }
                }

                JstormTopologyDTO jstormTopologyDTO = JStormTopologyBusiness.buildeTopologyDto(clusterSummary,topId);

                jstormTopologyDTO.setCompenentInfos(JStormTopologyBusiness.buildComponentInfo(topologyInfo,true));
                jstormTaskDetailDTO.setJstormTopologyDTO(jstormTopologyDTO);

                JstormProcessPO updateProcessPO = new JstormProcessPO();
                updateProcessPO.setId(processId);
                jstormTaskDetailDTO.setSavepointType(savepointType);
                jstormTaskDetailDTO.setSavepointTime(DateUtil.getCurrentDateTime());
                updateProcessPO.setTaskDetail(JSONObject.toJSONString(jstormTaskDetailDTO));
                jstormEngineService.jstormProcessDao.update(updateProcessPO);
            }else{
                //只更新 savepointType
                updateSavePointType(savepointType, processId);
            }
        }catch(Exception e){
            LOGGER.error("JstormEngineServiceImpl processTaskSavepoint is error",e);
        }
    }

    /**
     * 更新SavePointType
     * @param savepointType
     * @param processId
     */
    private static void updateSavePointType(Integer savepointType, Integer processId) {
        JstormProcessPO jstormProcessPO = jstormEngineService.jstormProcessDao.getById(processId);
        JstormTaskDetailDTO jstormTaskDetailDTO = JSONObject.parseObject(jstormProcessPO.getTaskDetail(), JstormTaskDetailDTO.class);
        if(jstormTaskDetailDTO!=null){
            JstormProcessPO updateProcessPO = new JstormProcessPO();
            updateProcessPO.setId(processId);

            jstormTaskDetailDTO.setSavepointType(savepointType);
            jstormTaskDetailDTO.setSavepointTime(DateUtil.getCurrentDateTime());
            updateProcessPO.setTaskDetail(JSONObject.toJSONString(jstormTaskDetailDTO));
            jstormEngineService.jstormProcessDao.update(updateProcessPO);
        }
    }
}
