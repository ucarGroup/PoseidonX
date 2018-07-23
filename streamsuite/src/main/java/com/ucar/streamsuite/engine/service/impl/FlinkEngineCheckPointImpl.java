package com.ucar.streamsuite.engine.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.*;
import com.ucar.streamsuite.common.util.*;
import com.ucar.streamsuite.dao.mysql.FlinkProcessDao;

import com.ucar.streamsuite.dao.mysql.TaskDao;
import com.ucar.streamsuite.engine.business.*;
import com.ucar.streamsuite.engine.constants.EngineContant;
import com.ucar.streamsuite.engine.dto.FlinkJobDTO;
import com.ucar.streamsuite.engine.dto.FlinkTaskConfigDTO;
import com.ucar.streamsuite.engine.po.FlinkProcessPO;
import com.ucar.streamsuite.moniter.business.AlarmBusiness;

import com.ucar.streamsuite.moniter.business.impl.MetricReportContainer;
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
 * Description: flink任务检查点
 * Created on 2018/1/30 上午10:59
 *
 *
 */
@Component
public class FlinkEngineCheckPointImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkEngineCheckPointImpl.class);

    private static final ScheduledExecutorService checkPointer = Executors.newScheduledThreadPool(1);

    private static final LinkedBlockingQueue<String> waitRecoveryQueue = new LinkedBlockingQueue<String>(5000);

    //使用的线程池个数较大 要保证规定时间内执行完
    private static final ExecutorService checkPointExecutorService = Executors.newFixedThreadPool(20);

    //防止恢复时重复提交
    private static ConcurrentHashMap<String,FlinkTaskConfigDTO> taskRecoverying = new ConcurrentHashMap<String,FlinkTaskConfigDTO>();

    //恢复失败历史。防止恢复死循环发生
    public static Set<Integer> recoveryFail = new HashSet<Integer>();

    private static FlinkEngineCheckPointImpl flinkEngineCheckPointImpl;

    @Autowired
    private TaskDao taskDao;
    @Autowired
    private UserService userService;
    @Autowired
    private FlinkProcessDao flinkProcessDao;

    private static AlarmBusiness alarmBusiness = null;

    static{
        startCheckPointer();
        startRecoveryListener();
    }

    //初始化静态参数
    @PostConstruct
    public void init() {
        flinkEngineCheckPointImpl = this;

        String alermClass = ConfigProperty.getProperty(ConfigProperty.MONITER_ALARM_CLASS);
        if(StringUtils.isNotBlank(alermClass)){
            try{
                alarmBusiness = (AlarmBusiness)Class.forName(alermClass).newInstance();
            }catch(Exception e){
                LOGGER.error("FlinkEngineCheckPointImpl init alarmBusiness error", e);
            }
        }
    }

    /**
     * 执行任务检查点
     */
    private static class CheckPointExecutor implements Runnable {

        private TaskPO taskPO;
        private FlinkProcessPO flinkProcessPO;

        public CheckPointExecutor(TaskPO taskPO,FlinkProcessPO flinkProcessPO) {
            this.taskPO = taskPO;
            this.flinkProcessPO = flinkProcessPO;
        }

        public void run() {
            executeCheckPoint(taskPO,flinkProcessPO);
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
                    long begin = System.currentTimeMillis();

                    Map<TaskPO,FlinkProcessPO> taskToProcessMap = FlinkEngineServiceImpl.listAllTaskToProcess();
                    for(Map.Entry<TaskPO,FlinkProcessPO> taskToProcess: taskToProcessMap.entrySet()){
                        CheckPointExecutor checkPointExecutor = new CheckPointExecutor(taskToProcess.getKey(),taskToProcess.getValue());
                        checkPointExecutorService.submit(checkPointExecutor);
                    }

                    //LOGGER.error("FlinkEngineCheckPointImpl checkPointer end" + " exeTime = " + (System.currentTimeMillis() - begin) /1000);
                } catch (Throwable e) {
                    LOGGER.error("FlinkEngineCheckPointImpl checkPointer is error", e);
                }
            }
        }, 1, EngineContant.FLINK_CHECKPOINT_CYCLE, TimeUnit.SECONDS);
    }

    /**
     * 开始JOB恢复者。当检查点发现需要回复会提交给任务恢复者进行恢复
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
                        LOGGER.error("FlinkEngineCheckPointImpl startRecoveryListener is error", e);
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
    private static void submitRecovery(FlinkTaskConfigDTO taskConfigDTO) {
        if(taskRecoverying.putIfAbsent(taskConfigDTO.getTaskName(),taskConfigDTO) == null){
            try {
                if (!waitRecoveryQueue.offer(taskConfigDTO.getTaskName(), 1, TimeUnit.SECONDS)) {
                    taskRecoverying.remove(taskConfigDTO.getTaskName());
                    LOGGER.error("FlinkEngineCheckPointImpl submitRecovery offer is error waitRecoveryQueue is full !!!!!");
                }else{
                    LOGGER.error("FlinkEngineCheckPointImpl submitRecovery offer is ok taskName =" + taskConfigDTO.getTaskName());
                }
            } catch (Exception e) {
                taskRecoverying.remove(taskConfigDTO.getTaskName());
            }
        }
    }

    /**
     * 执行检查点功能
     */
    private static void executeCheckPoint(TaskPO taskPO, FlinkProcessPO flinkProcessPO) {
        try{
            if(taskPO.getTaskStatus() != TaskStatusEnum.RUNNING.getValue()){
                return;
            }

            //正在恢复的跳过处理
            if(taskRecoverying.containsKey(taskPO.getTaskName())){
                return;
            }

            //数据错误跳过处理
            FlinkTaskConfigDTO taskConfigDTO = JSONObject.parseObject(flinkProcessPO.getTaskConfig(), FlinkTaskConfigDTO.class);
            if(taskConfigDTO == null){
                return;
            }

            ApplicationReport applicationReport = YarnClientProxy.getApplicationReportByAppId(flinkProcessPO.getYarnAppId());
            if(applicationReport==null){

                // 双重检查，多判断一次如果已经停止，直接返回，防止误报
                if(getTaskWhenIsNotStop(taskPO.getId(), taskPO.getTaskName()) == null){
                    return;
                }
                //恢复之前进行保存
                FlinkEngineServiceImpl.processTaskSavepoint(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId(),FlinkJobSavePointEnum.APP_ERROR.getValue(),flinkProcessPO.getId());

                TaskPO uptaskPO = new TaskPO();
                uptaskPO.setId(taskPO.getId());
                uptaskPO.setErrorInfo(JSONObject.toJSONString(Sets.newHashSet("yarn environment cant not connect! "+ "[" + DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime() + "]"))));
                flinkEngineCheckPointImpl.taskDao.update4ERROR(uptaskPO);

                submitRecovery(taskConfigDTO);
                return;
            }

            FlinkJobDTO flinkJobDTO = FlinkWebClientBusiness.getFlinkJobDtoWithRetry(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId());
            if(flinkJobDTO == null){

                // 双重检查，多判断一次如果已经停止，直接返回，防止误报
                if(getTaskWhenIsNotStop(taskPO.getId(), taskPO.getTaskName()) == null){
                    return;
                }

                //恢复之前进行保存
                FlinkEngineServiceImpl.processTaskSavepoint(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId(),FlinkJobSavePointEnum.JOB_ERROR.getValue(),flinkProcessPO.getId());

                TaskPO uptaskPO = new TaskPO();
                uptaskPO.setId(taskPO.getId());
                uptaskPO.setErrorInfo(JSONObject.toJSONString(Sets.newHashSet("flink job cant not connect! " + "[" + DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime() + "]"))));
                flinkEngineCheckPointImpl.taskDao.update4ERROR(uptaskPO);

                submitRecovery(taskConfigDTO);
                return;
            }

            //进行保存job信息
            FlinkEngineServiceImpl.processTaskSavepoint(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId(),FlinkJobSavePointEnum.COMMON.getValue(),flinkProcessPO.getId());

            if(flinkJobDTO.getState().equals("FAILED")){

                // 双重检查，多判断一次如果已经停止，直接返回，防止误报
                if(getTaskWhenIsNotStop(taskPO.getId(), taskPO.getTaskName()) == null){
                    return;
                }

                TaskPO uptaskPO = new TaskPO();
                uptaskPO.setId(taskPO.getId());
                uptaskPO.setErrorInfo(JSONObject.toJSONString(Sets.newHashSet("flink job is FAILED!" + "[" + DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime() + "]" ))));
                uptaskPO.setTaskStatus(TaskStatusEnum.ERROR.getValue());
                uptaskPO.setTaskStopTime(new Date());
                flinkEngineCheckPointImpl.taskDao.update4ERROR(uptaskPO);

                String EMAIL_ALARM_OPEN = ConfigProperty.getConfigValue(ConfigKeyEnum.EMAIL_ALARM_OPEN);
                String PHONE_ALARM_OPEN = ConfigProperty.getConfigValue(ConfigKeyEnum.PHONE_ALARM_OPEN);
                String ENVIRONMENT = ConfigProperty.getProperty(ConfigProperty.ENVIRONMENT);

                //进行提醒
                Map<String,String> taskRelationUsers = flinkEngineCheckPointImpl.userService.listTaskRelationUserTels(taskPO.getId());
                String emainTitle = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息";
                String emainContent = "Flink 任务 [" + taskPO.getTaskName() + "] 的 job is FAILED ，请及时进行手动任务恢复。" + "[当前时间为：" + DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime()) +"]";
                String phoneContent = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息：" + emainContent;
                if(StringUtils.isNotBlank(EMAIL_ALARM_OPEN) && EMAIL_ALARM_OPEN.equals("1")){
                    alarmBusiness.sendEmail(taskRelationUsers.keySet(),emainTitle,emainContent);
                }
                if(StringUtils.isNotBlank(PHONE_ALARM_OPEN) && PHONE_ALARM_OPEN.equals("1")){
                    alarmBusiness.sendPhone(Sets.<String>newHashSet(taskRelationUsers.values()),phoneContent);
                }
            }

            //刷新错误信息
            String jobRootException = FlinkWebClientBusiness.getJobRootException(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId());
            if(StringUtils.isNotBlank(jobRootException)){
                TaskPO uptaskPO = new TaskPO();
                uptaskPO.setId(taskPO.getId());
                uptaskPO.setErrorInfo(JSONObject.toJSONString(Sets.newHashSet(StringUtils.split(jobRootException,"\n\t"))));
                flinkEngineCheckPointImpl.taskDao.update4ERROR(uptaskPO);
            }
        }catch (Throwable e){
            LOGGER.error("FlinkEngineCheckPointImpl executeCheckPoint is error",e);
        }
    }

    /**
     * 任务恢复
     * @param flinkTaskConfigDTO
     */
    private static void executeRecovery(FlinkTaskConfigDTO flinkTaskConfigDTO) {
        TaskPO taskPO = null;
        boolean recoveryFailed = true;
        String recoveryRetry = "0";
        //是否需要恢复，无需恢复时不发短信
        boolean needRecovery = true;
        FlinkProcessPO flinkProcessPO = null;
        try{
            if(recoveryFail.contains(flinkTaskConfigDTO.getTaskId())){
               needRecovery = false;
               return;
            }

            taskPO = getTaskWhenIsNotStop(flinkTaskConfigDTO.getTaskId(),flinkTaskConfigDTO.getTaskName());
            if (taskPO == null) {
                LOGGER.error("jobRecovery is return so task is Stoped=" + flinkTaskConfigDTO.getTaskId());
                needRecovery = false;
                return;
            }

            flinkProcessPO = flinkEngineCheckPointImpl.flinkProcessDao.getById(taskPO.getProcessId());
            if(flinkProcessPO == null){
                needRecovery = false;
                return;
            }

            killAppBeforeRecovery(flinkProcessPO,taskPO);

            //进行恢复，每3分钟重试一次恢复，重试次数可配，默认为3次
            recoveryRetry = ConfigProperty.getConfigValue(ConfigKeyEnum.FLINK_AUTO_RECOVERY_RETRY);
            if(StringUtils.trimToEmpty(recoveryRetry).equals("")){
                recoveryRetry = "3";
            }
            int i = 1;
            Integer retryTimes = Integer.valueOf(recoveryRetry);
            while (true) {
                LOGGER.error("jobRecovery taskSubmit ready retryTimes=" + i + " taskName=" + taskPO.getTaskName());
                if(EngineBusiness.Submitter.taskSubmit(flinkTaskConfigDTO.getTaskName() ,flinkTaskConfigDTO, TaskSubmitTypeEnum.RECOVRY)){
                    LOGGER.error("jobRecovery taskSubmit complete retryTimes=" + i  + " taskName=" + taskPO.getTaskName());
                    recoveryFailed = false;
                    break;
                }else{
                    LOGGER.error("jobRecovery taskSubmit failed retryTimes=" + i  + " taskName=" + taskPO.getTaskName());
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
            LOGGER.error("jobRecovery is error",e);
        }finally {
            try{

                if(taskPO != null && needRecovery){
                    String EMAIL_ALARM_OPEN = ConfigProperty.getConfigValue(ConfigKeyEnum.EMAIL_ALARM_OPEN);
                    String PHONE_ALARM_OPEN = ConfigProperty.getConfigValue(ConfigKeyEnum.PHONE_ALARM_OPEN);
                    String ENVIRONMENT = ConfigProperty.getProperty(ConfigProperty.ENVIRONMENT);

                    if(recoveryFailed){

                        LOGGER.error("jobRecovery topologySubmit failed taskName=" + taskPO.getTaskName());

                        recoveryFail.add(taskPO.getId());

                        //恢复失败之前进行保存
                        FlinkEngineServiceImpl.processTaskSavepoint(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId(),FlinkJobSavePointEnum.ALL_ERROR.getValue(),flinkProcessPO.getId());

                        TaskPO updateTaskPo = new TaskPO();
                        updateTaskPo.setId(taskPO.getId());
                        updateTaskPo.setTaskStatus(TaskStatusEnum.ERROR.getValue());
                        updateTaskPo.setTaskStopTime(new Date());
                        flinkEngineCheckPointImpl.taskDao.update4ERROR(updateTaskPo);

                        Map<String,String> taskRelationUsers = flinkEngineCheckPointImpl.userService.listTaskRelationUserTels(taskPO.getId());
                        //进行提醒
                        String emainTitle = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息";
                        String emainContent = "Flink任务 [" + taskPO.getTaskName() + "] 异常终止，且尝试自动恢复" + recoveryRetry + "次后恢复失败，请及时进行手动任务恢复。" + " [当前时间为：" +  DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime()) +"]";
                        String phoneContent = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息：" + emainContent;
                        if(StringUtils.isNotBlank(EMAIL_ALARM_OPEN) && EMAIL_ALARM_OPEN.equals("1")){
                            alarmBusiness.sendEmail(taskRelationUsers.keySet(),emainTitle,emainContent);
                        }
                        if(StringUtils.isNotBlank(PHONE_ALARM_OPEN) && PHONE_ALARM_OPEN.equals("1")){
                            alarmBusiness.sendPhone(Sets.<String>newHashSet(taskRelationUsers.values()),phoneContent);
                        }
                    }else{
                        Map<String,String> taskRelationUsers = flinkEngineCheckPointImpl.userService.listTaskRelationUserTels(taskPO.getId());
                        //进行提醒
                        String emainTitle = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息";
                        String emainContent = "Flink任务 [" + taskPO.getTaskName() + "] 异常终止后自动恢复成功，请及时登录平台查看任务失败原因。" + " [当前时间为：" +  DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime()) +"]";
                        String phoneContent = "来自'新版波塞冬平台["+ ENVIRONMENT+ "]'的报警信息：" + emainContent;
                        if(StringUtils.isNotBlank(EMAIL_ALARM_OPEN) && EMAIL_ALARM_OPEN.equals("1")){
                            alarmBusiness.sendEmail(taskRelationUsers.keySet(),emainTitle,emainContent);
                        }
                        if(StringUtils.isNotBlank(PHONE_ALARM_OPEN) && PHONE_ALARM_OPEN.equals("1")){
                            alarmBusiness.sendPhone(Sets.<String>newHashSet(taskRelationUsers.values()),phoneContent);
                        }
                    }
                }
            } catch (Exception e){
                LOGGER.error("jobRecovery is error",e);
            }
        }
    }

    /**
     * 停止任在恢复之前
     * @param flinkProcessPO
     * @throws Exception
     */
    private static void killAppBeforeRecovery(FlinkProcessPO flinkProcessPO,TaskPO taskPO) throws Exception {

        LOGGER.error("jobRecovery ready killApplication appId=" + flinkProcessPO.getYarnAppId() + " taskName=" + taskPO.getTaskName());

        //彻底删除老任务
        boolean stopIsOk = FlinkOnYarnBusiness.stopJob(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId());
        if(!stopIsOk){
            throw new Exception("jobRecovery stopJob is error");
        }
        ApplicationReport applicationReport = YarnClientProxy.getApplicationReportByAppId(flinkProcessPO.getYarnAppId());
        if(applicationReport !=null){
            try{
                YarnClientProxy.killApplicationByAppId(flinkProcessPO.getYarnAppId());
            }catch (Exception e){
                throw new Exception("jobRecovery killApplication is error",e);
            }
        }
        // 清理资源
        MetricReportContainer.removeReports(flinkProcessPO.getJobId());
        FlinkEngineCheckPointImpl.recoveryFail.remove(flinkProcessPO.getTaskId());

        LOGGER.error("jobRecovery killApplication complete appId=" + flinkProcessPO.getYarnAppId() + " taskName=" + taskPO.getTaskName());

    }

    /**
     * 当未停止，或不是正在停止的的时候，返回任务
     * @param taskId
     * @param taskName
     * @return
     * @throws Exception
     */
    private static TaskPO getTaskWhenIsNotStop(Integer taskId,String taskName) throws Exception{
        TaskPO taskPO = flinkEngineCheckPointImpl.taskDao.getById(taskId);
        if(taskPO == null || taskPO.getTaskStatus().equals(TaskStatusEnum.STOP.getValue())){
            return null;
        }
        CuratorFramework cfClient = ZKUtil.getClient(StreamContant.HDFS_HADOOP_ZOOKEEPER);
        if(CollectionUtils.isNotEmpty(ZKUtil.getChildrens(cfClient, EngineContant.TASK_STOP_LOCK_PRE + taskName))){
            return null;
        }
        return taskPO;
    }
}
