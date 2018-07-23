package com.ucar.streamsuite.engine.service.impl;

import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.*;
import com.ucar.streamsuite.common.util.*;
import com.ucar.streamsuite.dao.mysql.JstormProcessDao;
import com.ucar.streamsuite.dao.mysql.TaskDao;
import com.ucar.streamsuite.engine.business.EngineBusiness;
import com.ucar.streamsuite.engine.business.JStormClusterBusiness;
import com.ucar.streamsuite.engine.business.JStormOnYarnBusiness;
import com.ucar.streamsuite.engine.business.JStormTopologyBusiness;

import com.ucar.streamsuite.engine.constants.EngineContant;
import com.ucar.streamsuite.engine.constants.YarnZkContant;
import com.ucar.streamsuite.engine.dto.AppContainerIdDTO;
import com.ucar.streamsuite.engine.dto.JstormTaskConfigDTO;
import com.ucar.streamsuite.engine.dto.JstormTopologyDTO;
import com.ucar.streamsuite.engine.po.JstormProcessPO;

import com.ucar.streamsuite.moniter.business.AlarmBusiness;
import com.ucar.streamsuite.moniter.business.impl.MetricReportContainer;
import com.ucar.streamsuite.moniter.service.impl.JstormMetricCollectImpl;
import com.ucar.streamsuite.task.po.TaskPO;

import com.ucar.streamsuite.user.service.UserService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;

import java.util.*;
import java.util.concurrent.*;

/**
 * Description: jstorm任务检查点
 * Created on 2018/1/30 上午10:59
 *
 *
 */
@Component
public class JstormEngineCheckPointImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(JstormEngineCheckPointImpl.class);

    private static final ScheduledExecutorService checkPointer = Executors.newScheduledThreadPool(1);

    private static final LinkedBlockingQueue<String> waitRecoveryQueue = new LinkedBlockingQueue<String>(5000);

    //使用的线程池个数较大 要保证规定时间内执行完
    private static final ExecutorService checkPointExecutorService = Executors.newFixedThreadPool(20);

    //防止恢复时重复提交
    private static ConcurrentHashMap<String,JstormTaskConfigDTO> taskRecoverying = new ConcurrentHashMap<String,JstormTaskConfigDTO>();

    //恢复失败历史。防止恢复死循环发生，需要在停止任务的时候清除
    public static Set<Integer> recoveryFail = new HashSet<Integer>();

    private static JstormEngineCheckPointImpl jstormEngineRecoveryService;

    @Autowired
    private TaskDao taskDao;
    @Autowired
    private UserService userService;
    @Autowired
    private JstormProcessDao jstormProcessDao;

    private static AlarmBusiness alarmBusiness = null;

    static{
        startCheckPointer();
        startRecoveryListener();
    }

    //初始化静态参数
    @PostConstruct
    public void init() {
        jstormEngineRecoveryService = this;

        String alermClass = ConfigProperty.getProperty(ConfigProperty.MONITER_ALARM_CLASS);
        if(StringUtils.isNotBlank(alermClass)){
            try{
                alarmBusiness = (AlarmBusiness)Class.forName(alermClass).newInstance();
            }catch(Exception e){
                LOGGER.error("JstormEngineCheckPointImpl init alarmBusiness error", e);
            }
        }
    }

    /**
     * 执行Jstorm任务检查点
     */
    private static class CheckPointExecutor implements Runnable {
        private TaskPO taskPO;
        private JstormProcessPO jstormProcessPO;
        private Map<String, Set<AppContainerIdDTO>> appMetadatas;
        public CheckPointExecutor(TaskPO taskPO,JstormProcessPO jstormProcessPO,Map<String, Set<AppContainerIdDTO>> appMetadatas) {
            this.taskPO = taskPO;
            this.jstormProcessPO = jstormProcessPO;
            this.appMetadatas = appMetadatas;
        }
        public void run() {
            executeCheckPoint(taskPO,jstormProcessPO,appMetadatas);
        }
    }

    /**
     * 开始检查点线程
     */
    private static void startCheckPointer(){
        /**
         * 开始检查点线程 1分钟执行一次
         */
        checkPointer.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!LeaderSelecter.getInstance().isLeader()) {
                        return;
                    }

                    //预先加载一次参数，避免在循环内加载
                    EngineContant.JSTORM_CLUSTER_RECOVERY_FLAG = ConfigProperty.getConfigValue(ConfigKeyEnum.JSTORM_CLUSTER_RECOVERY_FLAG);
                    Map<TaskPO,JstormProcessPO> taskToProcessMap = JstormEngineServiceImpl.listAllTaskToProcess();

                    Map<String,Set<AppContainerIdDTO>> appMetadatas = synAndGetAppMetadata(taskToProcessMap);

                    for(Map.Entry<TaskPO,JstormProcessPO> taskToProcess: taskToProcessMap.entrySet()){
                        CheckPointExecutor checkPointExecutor = new CheckPointExecutor(taskToProcess.getKey(),taskToProcess.getValue(),appMetadatas);
                        checkPointExecutorService.submit(checkPointExecutor);
                    }

                } catch (Throwable e) {
                    LOGGER.error("JstormEngineCheckPointImpl checkPointer is error", e);
                }
            }
        }, 1, EngineContant.JSTORM_CHECKPOINT_CYCLE, TimeUnit.SECONDS);
    }

    /**
     * 开始拓扑恢复者。当检查点发现需要回复会提交给任务恢复者进行恢复
     */
    private static void startRecoveryListener(){
        waitRecoveryQueue.clear();
        taskRecoverying.clear();
        recoveryFail.clear();
        Thread recoveryListener = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    String taskName = "";
                    try {
                        TimeUnit.SECONDS.sleep(5);
                        if (!LeaderSelecter.getInstance().isLeader()) {
                            continue;
                        }
                        taskName = waitRecoveryQueue.poll(3, TimeUnit.SECONDS);
                        if (StringUtils.isBlank(taskName)) {
                            continue;
                        }
                        if(taskRecoverying.containsKey(taskName)){
                            executeRecovery(taskRecoverying.get(taskName));
                            taskRecoverying.remove(taskName);
                        }
                    } catch (Throwable e) {
                        if(StringUtils.isNotBlank(taskName)){
                            taskRecoverying.remove(taskName);
                        }
                        LOGGER.error("JstormEngineCheckPointImpl startRecoveryListener is error", e);
                    }
                }
            }
        });
        recoveryListener.setDaemon(true);
        recoveryListener.start();
    }

    /**
     * 提交恢复
     * @param taskConfigDTO
     */
    private static void submitRecovery(JstormTaskConfigDTO taskConfigDTO) {
        if(taskRecoverying.putIfAbsent(taskConfigDTO.getTaskName(),taskConfigDTO) == null){
            try {
                if (!waitRecoveryQueue.offer(taskConfigDTO.getTaskName(), 1, TimeUnit.SECONDS)) {
                    taskRecoverying.remove(taskConfigDTO.getTaskName());
                    LOGGER.error("JstormEngineCheckPointImpl submitRecovery offer is error waitRecoveryQueue is full !!!!!");
                }else{
                    LOGGER.error("JstormEngineCheckPointImpl submitRecovery offer is ok taskName =" + taskConfigDTO.getTaskName());
                }
            } catch (Exception e) {
                taskRecoverying.remove(taskConfigDTO.getTaskName());
            }
        }
    }

    /**
     * 同步当前 app元数据刷新zk信息。任何任务状态的任务都需要执行刷新zk信息，因此放在执行检查点之前执行这块代码
     * @param taskToProcessMap
     */
    private static Map<String,Set<AppContainerIdDTO>> synAndGetAppMetadata(Map<TaskPO,JstormProcessPO> taskToProcessMap){
        //所以在用的并且zk上也有效的container
        Set<AppContainerIdDTO> allInUseZkContainers = Sets.newHashSet();
        Map<String,Set<AppContainerIdDTO>> appIdToZkContainers = Maps.newHashMap();

        for(Map.Entry<TaskPO,JstormProcessPO> taskToProcess: taskToProcessMap.entrySet()){
            String yarnAppId = taskToProcess.getValue().getYarnAppId();
            String taskName = taskToProcess.getKey().getTaskName();

            Set<AppContainerIdDTO> inUseZkContainers = JStormOnYarnBusiness.refreshAndGetZkAppContainer(yarnAppId,taskName);

            allInUseZkContainers.addAll(inUseZkContainers);
            appIdToZkContainers.put(taskToProcess.getValue().getYarnAppId(),inUseZkContainers);
        }
        JStormOnYarnBusiness.refreshInUsePortToZk(allInUseZkContainers);
        return appIdToZkContainers;
    }

    /**
     * 执行检查点功能
     */
    private static void executeCheckPoint(TaskPO taskPO, JstormProcessPO jstormProcessPO,Map<String, Set<AppContainerIdDTO>> appMetadatas) {
        NimbusClient nimbusClient = null;
        try{
            if(taskPO.getTaskStatus() != TaskStatusEnum.RUNNING.getValue()){
                return;
            }

            //正在恢复的跳过处理
            if(taskRecoverying.containsKey(taskPO.getTaskName())){
                return;
            }

            //数据错误跳过处理
            JstormTaskConfigDTO taskConfigDTO = JSONObject.parseObject(jstormProcessPO.getTaskConfig(), JstormTaskConfigDTO.class);
            if(taskConfigDTO == null){
                return;
            }

            Set<AppContainerIdDTO> currentAppMetadata = appMetadatas.get(jstormProcessPO.getYarnAppId());
            if(CollectionUtils.isEmpty(currentAppMetadata)){

                //如果运行中的任务，获得不到app，则进行恢复。
                ApplicationReport applicationReport = YarnClientProxy.getApplicationReportByAppId(jstormProcessPO.getYarnAppId());
                if(applicationReport == null){

                    // 保存错误信息
                    TaskPO uptaskPO = new TaskPO();
                    uptaskPO.setId(taskPO.getId());
                    uptaskPO.setErrorInfo(JSONObject.toJSONString(Sets.newHashSet("yarn environment cant not connect! "+  "[" + DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime() + "]"))));
                    jstormEngineRecoveryService.taskDao.update4ERROR(uptaskPO);

                    //app异常，恢复之前进行保存
                    JstormEngineServiceImpl.processTaskSavepoint(jstormProcessPO.getYarnAppId(),jstormProcessPO.getTopId(),JstormTopologySavePointEnum.APP_ERROR.getValue(),jstormProcessPO.getId(),taskConfigDTO);

                    //进行恢复提交
                    submitRecovery(taskConfigDTO);
                }
                return;
            }

            List<AppContainerIdDTO> oldAppMetadata = JSONObject.parseArray(jstormProcessPO.getYarnAppMetadata(),AppContainerIdDTO.class);
            if(oldAppMetadata == null){
                return;
            }

            String oldNimbusContainerId = "";
            for(AppContainerIdDTO oldContainerInfo: oldAppMetadata){
                if(oldContainerInfo.getContainerType() == JstormContainerTypeEnum.NIMBUS){
                    oldNimbusContainerId = oldContainerInfo.getContainerId();
                }
            }

            String currentNimbusContainerId = "";
            String currentNimbushost = "";
            for(AppContainerIdDTO currentContainerInfo: currentAppMetadata){
                if(currentContainerInfo.getContainerType() == JstormContainerTypeEnum.NIMBUS){
                    currentNimbusContainerId = currentContainerInfo.getContainerId();
                    currentNimbushost = currentContainerInfo.getServiceRecord().get(YarnZkContant.ZK_CONTAINER_JSTORM_HOST);
                }
            }

            //如果数据有问题直接返回
            if(StringUtils.isBlank(currentNimbusContainerId) || StringUtils.isBlank(oldNimbusContainerId) || StringUtils.isBlank(currentNimbushost)){
                return;
            }

            nimbusClient = JStormClusterBusiness.getNimBusClientWithRetry(taskConfigDTO.getJstormZkHost(),taskConfigDTO.getJstormZkPort(),taskConfigDTO.getJstormZkRoot(),2,2);
            if(nimbusClient == null){

                // 保存错误信息
                TaskPO uptaskPO = new TaskPO();
                uptaskPO.setId(taskPO.getId());
                uptaskPO.setErrorInfo(JSONObject.toJSONString(Sets.newHashSet("jstorm nimbus can not connect! " +  "[" + DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime() + "]"))));
                jstormEngineRecoveryService.taskDao.update4ERROR(uptaskPO);

                //这里是兜底的情况。也就是说只要发现集群的nimbus有死亡的情况就进行恢复重提。默认为false。
                boolean needRecovery = (StringUtils.trimToEmpty(EngineContant.JSTORM_CLUSTER_RECOVERY_FLAG)).equals("1");
                String sshPassword = EngineContant.HADOOP_USER_PASSWORD;

                if(StringUtils.isNotBlank(sshPassword)){
                    if(needRecovery && !JStormClusterBusiness.existNimbusProcess(currentNimbushost, EngineContant.SSH_DEFAULT_PORT, StreamContant.HADOOP_USER_NAME, sshPassword)){
                        //nimbusClient异常，恢复之前进行保存
                        JstormEngineServiceImpl.processTaskSavepoint(jstormProcessPO.getYarnAppId(),jstormProcessPO.getTopId(),JstormTopologySavePointEnum.NIMBUS_ERROR.getValue(),jstormProcessPO.getId(),taskConfigDTO);
                        //进行恢复
                        submitRecovery(taskConfigDTO);
                        return;
                    }
                }else{
                    if(needRecovery){
                        //nimbusClient异常，恢复之前进行保存
                        JstormEngineServiceImpl.processTaskSavepoint(jstormProcessPO.getYarnAppId(),jstormProcessPO.getTopId(),JstormTopologySavePointEnum.NIMBUS_ERROR.getValue(),jstormProcessPO.getId(),taskConfigDTO);
                        //进行恢复
                        submitRecovery(taskConfigDTO);
                        return;
                    }
                }
            }

            //如果nimbus的container变了。 这种判断已经覆盖了两种情况
            //1 nimbus 异常死亡：AM创建新的container，然后streamsuite会重提任务
            //2 Am 异常死亡：AM会被NM重启，此时zk上的container会出现新的一组tempID的container节点。AM创建新的container，streamsuite会重提任务。
            //3 NM 进程异常死亡：RM会在其他NM机器重启AM，streamsuite会重提任务。

            if(!oldNimbusContainerId.equals(currentNimbusContainerId)){

                // 保存错误信息
                TaskPO uptaskPO = new TaskPO();
                uptaskPO.setId(taskPO.getId());
                uptaskPO.setErrorInfo(JSONObject.toJSONString(Sets.newHashSet("jstorm nimbus container is changed! "  +  "[" + DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime() + "]"))));
                jstormEngineRecoveryService.taskDao.update4ERROR(uptaskPO);

                //nimbus的container变了，恢复之前进行保存
                JstormEngineServiceImpl.processTaskSavepoint(jstormProcessPO.getYarnAppId(),jstormProcessPO.getTopId(),JstormTopologySavePointEnum.NIMBUS_CHANGE.getValue(),jstormProcessPO.getId(),taskConfigDTO);

                //进行恢复
                submitRecovery(taskConfigDTO);
                return;
            }

            Map<String,String> taskRelationUsers = jstormEngineRecoveryService.userService.listTaskRelationUserTels(taskPO.getId());
            String EMAIL_ALARM_OPEN = ConfigProperty.getConfigValue(ConfigKeyEnum.EMAIL_ALARM_OPEN);
            String PHONE_ALARM_OPEN = ConfigProperty.getConfigValue(ConfigKeyEnum.PHONE_ALARM_OPEN);
            String ENVIRONMENT = ConfigProperty.getProperty(ConfigProperty.ENVIRONMENT);

            // 如果没有任何恢复处理，但是集群连不上
            if(nimbusClient == null){

                // 双重检查，多判断一次如果已经停止，直接返回，防止误报
                if(getTaskWhenIsNotStop(taskPO.getId(), taskPO.getTaskName()) == null){
                    return;
                }

                // 保存错误信息
                TaskPO uptaskPO = new TaskPO();
                uptaskPO.setId(taskPO.getId());
                uptaskPO.setErrorInfo(JSONObject.toJSONString(Sets.newHashSet("jstorm nimbus can not connect! "  +  "[" + DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime() + "]"))));
                uptaskPO.setTaskStatus(TaskStatusEnum.ERROR.getValue());
                uptaskPO.setTaskStopTime(new Date());
                jstormEngineRecoveryService.taskDao.update4ERROR(uptaskPO);

                //保存任务详情信息：集群信息Inactive，nimbus 状态为Inactive
                JstormEngineServiceImpl.processTaskSavepoint(jstormProcessPO.getYarnAppId(),jstormProcessPO.getTopId(),JstormTopologySavePointEnum.NIMBUS_ERROR.getValue(),jstormProcessPO.getId(),taskConfigDTO);

                //进行提醒
                String emainTitle = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息";
                String emainContent = "Jstorm任务 [" + taskPO.getTaskName() + "] 的 Nimbus进程已异常终止，且未执行自动任务恢复流程，请及时进行手动任务恢复。" + "[当前时间为：" +  DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime()) +"]";
                String phoneContent = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息：" + emainContent;
                if(StringUtils.isNotBlank(EMAIL_ALARM_OPEN) && EMAIL_ALARM_OPEN.equals("1")){
                    alarmBusiness.sendEmail(taskRelationUsers.keySet(),emainTitle,emainContent);
                }
                if(StringUtils.isNotBlank(PHONE_ALARM_OPEN) && PHONE_ALARM_OPEN.equals("1")){
                    alarmBusiness.sendPhone(Sets.<String>newHashSet(taskRelationUsers.values()),phoneContent);
                }

            }else{

                JstormTopologyDTO jstormTopologyDTO = JStormTopologyBusiness.getTopologyIsRunningWithRetry(nimbusClient,2,2);
                if(jstormTopologyDTO == null){

                    // 双重检查，多判断一次如果已经停止，直接返回，防止误报
                    if(getTaskWhenIsNotStop(taskPO.getId(), taskPO.getTaskName()) == null){
                        return;
                    }

                    // 保存错误信息
                    TaskPO uptaskPO = new TaskPO();
                    uptaskPO.setId(taskPO.getId());
                    uptaskPO.setErrorInfo(JSONObject.toJSONString(Sets.newHashSet("jstorm Topology can not connect! " + "[" + DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime() + "]"))));
                    uptaskPO.setTaskStatus(TaskStatusEnum.ERROR.getValue());
                    uptaskPO.setTaskStopTime(new Date());
                    jstormEngineRecoveryService.taskDao.update4ERROR(uptaskPO);

                    //保存任务详情信息：更新拓扑信息 JStormTopologyBusiness.getTopologyByTopId
                    JstormEngineServiceImpl.processTaskSavepoint(jstormProcessPO.getYarnAppId(),jstormProcessPO.getTopId(),JstormTopologySavePointEnum.TOP_ERROR.getValue(),jstormProcessPO.getId(),taskConfigDTO);

                    //进行提醒
                    String emainTitle = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息";
                    String emainContent = "Jstorm任务 [" + taskPO.getTaskName() + "] topology 已异常终止。且未执行自动任务恢复流程，请及时进行手动任务恢复。" + "[当前时间为：" +  DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime()) +"]";
                    String phoneContent = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息：" + emainContent;
                    if(StringUtils.isNotBlank(EMAIL_ALARM_OPEN) && EMAIL_ALARM_OPEN.equals("1")){
                        alarmBusiness.sendEmail(taskRelationUsers.keySet(),emainTitle,emainContent);
                    }
                    if(StringUtils.isNotBlank(PHONE_ALARM_OPEN) && PHONE_ALARM_OPEN.equals("1")){
                        alarmBusiness.sendPhone(Sets.<String>newHashSet(taskRelationUsers.values()),phoneContent);
                    }

                }else{
                    //保存任务详情信息：刷新所有的任务信息，app信息，集群信息，拓扑信息
                    JstormEngineServiceImpl.processTaskSavepoint(jstormProcessPO.getYarnAppId(),jstormProcessPO.getTopId(),JstormTopologySavePointEnum.COMMON.getValue(),jstormProcessPO.getId(),taskConfigDTO);

                    // 刷新错误信息
                    if(jstormTopologyDTO.getErrorInfo().toUpperCase().equals("Y")){
                        TaskPO uptaskPO = new TaskPO();
                        uptaskPO.setId(taskPO.getId());
                        uptaskPO.setErrorInfo(getTopologyErrorInfo(nimbusClient,jstormTopologyDTO.getTopId()));
                        jstormEngineRecoveryService.taskDao.update4ERROR(uptaskPO);
                    }
                }
            }
        }catch (Exception e){
            LOGGER.error("JstormEngineCheckPointImpl executeCheckPoint is error",e);
        }finally {
           if(nimbusClient!=null){
               nimbusClient.close();
           }
        }
    }

    /**
     * 任务恢复
     * @param jstormTaskConfigDTO
     */
    private static void executeRecovery(JstormTaskConfigDTO jstormTaskConfigDTO) {
        TaskPO taskPO = null;
        boolean recoveryFailed = true;
        String recoveryRetry = "0";
        //是否需要恢复，无需恢复时不发短信
        boolean needRecovery = true;
        JstormProcessPO jstormProcessPO = null;
        try{
            // 恢复失败的不回复，防止循环恢复
            if(recoveryFail.contains(jstormTaskConfigDTO.getTaskId())){
               needRecovery = false;
               return;
            }

            taskPO = getTaskWhenIsNotStop(jstormTaskConfigDTO.getTaskId(),jstormTaskConfigDTO.getTaskName());
            if (taskPO == null) {
                LOGGER.error("topologyRecovery is return so task is Stoped=" + jstormTaskConfigDTO.getTaskId());
                needRecovery = false;
                return;
            }

            jstormProcessPO = jstormEngineRecoveryService.jstormProcessDao.getById(taskPO.getProcessId());
            if(jstormProcessPO == null){
                needRecovery = false;
                return;
            }

            killAppBeforeRecovery(jstormTaskConfigDTO, taskPO, jstormProcessPO);

            //进行恢复，每3分钟重试一次恢复，重试次数可配，默认为3次
            recoveryRetry = ConfigProperty.getConfigValue(ConfigKeyEnum.JSTORM_AUTO_RECOVERY_RETRY);
            if(StringUtils.trimToEmpty(recoveryRetry).equals("")){
                recoveryRetry = "3";
            }
            int i = 1;
            Integer retryTimes = Integer.valueOf(recoveryRetry);
            while (true) {
                LOGGER.error("topologyRecovery topologySubmit ready retryTimes=" + i + " taskName=" + taskPO.getTaskName());
                if(EngineBusiness.Submitter.taskSubmit( jstormTaskConfigDTO.getTaskName() ,jstormTaskConfigDTO, TaskSubmitTypeEnum.RECOVRY)){
                    LOGGER.error("topologyRecovery topologySubmit complete retryTimes=" + i  + " taskName=" + taskPO.getTaskName());
                    recoveryFailed = false;
                    break;
                }else{
                    LOGGER.error("topologyRecovery topologySubmit failed retryTimes=" + i  + " taskName=" + taskPO.getTaskName());
                }
                if (retryTimes-- <= 0) {
                    break;
                }
                try {
                    TimeUnit.MINUTES.sleep(3);
                } catch (InterruptedException e) {
                }
                i ++ ;
            }
        }catch (Throwable e){
            LOGGER.error("topologyRecovery is error",e);
        }finally {
            try{
                if(taskPO != null && needRecovery){
                    String EMAIL_ALARM_OPEN = ConfigProperty.getConfigValue(ConfigKeyEnum.EMAIL_ALARM_OPEN);
                    String PHONE_ALARM_OPEN = ConfigProperty.getConfigValue(ConfigKeyEnum.PHONE_ALARM_OPEN);
                    String ENVIRONMENT = ConfigProperty.getProperty(ConfigProperty.ENVIRONMENT);

                    if(recoveryFailed){

                        LOGGER.error("topologyRecovery topologySubmit failed taskName=" + taskPO.getTaskName());

                        recoveryFail.add(taskPO.getId());

                        //恢复失败之前保存
                        JstormEngineServiceImpl.processTaskSavepoint(jstormProcessPO.getYarnAppId(),jstormProcessPO.getTopId(),JstormTopologySavePointEnum.ALL_ERROR.getValue(),jstormProcessPO.getId(),jstormTaskConfigDTO);

                        TaskPO updateTaskPO = new TaskPO();
                        updateTaskPO.setId(taskPO.getId());
                        updateTaskPO.setTaskStatus(TaskStatusEnum.ERROR.getValue());
                        updateTaskPO.setTaskStopTime(new Date());
                        jstormEngineRecoveryService.taskDao.update4ERROR(updateTaskPO);

                        Map<String,String> taskRelationUsers = jstormEngineRecoveryService.userService.listTaskRelationUserTels(taskPO.getId());
                        //进行提醒
                        String emainTitle = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息";
                        String emainContent = "Jstorm任务 [" + taskPO.getTaskName() + "] 异常终止，且尝试自动恢复" + recoveryRetry + "次后恢复失败，请及时进行手动任务恢复。" + " [当前时间为：" + DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime()) +"]";
                        String phoneContent = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息：" + emainContent;
                        if(StringUtils.isNotBlank(EMAIL_ALARM_OPEN) && EMAIL_ALARM_OPEN.equals("1")){
                            alarmBusiness.sendEmail(taskRelationUsers.keySet(),emainTitle,emainContent);
                        }
                        if(StringUtils.isNotBlank(PHONE_ALARM_OPEN) && PHONE_ALARM_OPEN.equals("1")){
                            alarmBusiness.sendPhone(Sets.<String>newHashSet(taskRelationUsers.values()),phoneContent);
                        }
                    }else{

                        Map<String,String> taskRelationUsers = jstormEngineRecoveryService.userService.listTaskRelationUserTels(taskPO.getId());
                        String emainTitle = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息";
                        String emainContent = "Jstorm任务 [" + taskPO.getTaskName() + "] 异常终止后自动恢复成功，请及时登录平台查看任务失败原因。" + " [当前时间为：" +  DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime()) +"]";
                        String phoneContent = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息：" + emainContent;
                        if(StringUtils.isNotBlank(EMAIL_ALARM_OPEN) && EMAIL_ALARM_OPEN.equals("1")){
                            alarmBusiness.sendEmail(taskRelationUsers.keySet(),emainTitle,emainContent);
                        }
                        if(StringUtils.isNotBlank(PHONE_ALARM_OPEN) && PHONE_ALARM_OPEN.equals("1")){
                            alarmBusiness.sendPhone(Sets.<String>newHashSet(taskRelationUsers.values()),phoneContent);
                        }
                    }
                }
            }catch (Exception e){
                LOGGER.error("topologyRecovery is error",e);
            }
        }
    }

    /**
     * 停止任在恢复之前
     * @param flinkProcessPO
     * @throws Exception
     */
    private static void killAppBeforeRecovery(JstormTaskConfigDTO jstormTaskConfigDTO, TaskPO taskPO, JstormProcessPO jstormProcessPO) throws Exception {
        LOGGER.error("topologyRecovery ready killApplication appId=" + jstormProcessPO.getYarnAppId() + " taskName=" + taskPO.getTaskName());

        //彻底删除老任务
        JStormOnYarnBusiness.killApplication(jstormTaskConfigDTO.getJstormZkHost(),jstormTaskConfigDTO.getJstormZkPort(),jstormProcessPO.getYarnAppId(),taskPO.getTaskName());
        //清理资源
        JstormMetricCollectImpl.workerErrorHistoryCache.remove(jstormProcessPO.getTopId());
        MetricReportContainer.removeReports(jstormProcessPO.getTopId());
        JstormEngineCheckPointImpl.recoveryFail.remove(jstormProcessPO.getTaskId());

        LOGGER.error("topologyRecovery killApplication complete appId=" + jstormProcessPO.getYarnAppId() + " taskName=" + taskPO.getTaskName());
    }

    /**
     * 当未停止，或不是正在停止的的时候，返回任务
     * @param taskId
     * @param taskName
     * @return
     * @throws Exception
     */
    private static TaskPO getTaskWhenIsNotStop(Integer taskId,String taskName) throws Exception{
        TaskPO taskPO = jstormEngineRecoveryService.taskDao.getById(taskId);
        if(taskPO == null || taskPO.getTaskStatus().equals(TaskStatusEnum.STOP.getValue())){
            return null;
        }
        CuratorFramework cfClient = ZKUtil.getClient(StreamContant.HDFS_HADOOP_ZOOKEEPER);
        if(CollectionUtils.isNotEmpty(ZKUtil.getChildrens(cfClient, EngineContant.TASK_STOP_LOCK_PRE + taskName))){
            return null;
        }
        return taskPO;
    }

    /**
     * 返回拓扑错误信息
     * @param nimbusClient
     * @return
     */
    public static String getTopologyErrorInfo(NimbusClient nimbusClient,String topId){
        TopologyInfo topologyInfo = JStormClusterBusiness.getTopologyInfoWithRetry(nimbusClient,topId);
        if(topologyInfo!=null){
            Set<String> error = JStormTopologyBusiness.buildTopologyErrorInfo(topologyInfo);
            return JSONObject.toJSONString(error);
        }
        return JSONObject.toJSONString(Sets.newHashSet());
    }
}
