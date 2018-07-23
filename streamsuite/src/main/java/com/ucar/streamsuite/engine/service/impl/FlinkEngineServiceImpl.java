package com.ucar.streamsuite.engine.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ucar.flinksql.FlinkSqlUtils;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.*;
import com.ucar.streamsuite.common.util.DateUtil;
import com.ucar.streamsuite.common.util.YarnClientProxy;
import com.ucar.streamsuite.cql.po.CqlPO;
import com.ucar.streamsuite.dao.mysql.*;
import com.ucar.streamsuite.engine.business.*;
import com.ucar.streamsuite.engine.constants.EngineContant;
import com.ucar.streamsuite.engine.dto.FlinkJobDTO;
import com.ucar.streamsuite.engine.dto.FlinkTaskConfigDTO;
import com.ucar.streamsuite.engine.dto.FlinkTaskDetailDTO;
import com.ucar.streamsuite.engine.po.FlinkProcessPO;
import com.ucar.streamsuite.moniter.business.impl.MetricReportContainer;
import com.ucar.streamsuite.task.dto.TaskDTO;
import com.ucar.streamsuite.task.dto.TaskStartTimeLineDTO;
import com.ucar.streamsuite.task.po.TaskArchivePO;
import com.ucar.streamsuite.task.po.TaskArchiveVersionPO;
import com.ucar.streamsuite.task.po.TaskPO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;

/**
 * Description: flink任务服务类实现类
 * Created on 2018/1/30 上午10:59
 *
 *
 */
@Service
public class FlinkEngineServiceImpl {

    private final static String LOG_deployCLusterTimeOut = "Please check if the requested resources are available in the YARN cluster";
    private final static String LOG_deployingCLuster = "Deploying cluster";
    private final static String LOG_submitingApp = "Submitting application master";
    private final static String LOG_submittedApp = "Submitted application ";
    private final static String LOG_submitingJob = "Submitting Job";
    private final static String LOG_submittedJob = "Job has been submitted with JobID ";
    private final static String LOG_submittedAppSuccess = "YARN application has been deployed successfully.";
    private final static Integer START_SESSION_MAX_WAIT = 90; //秒

    private final static ExecutorService printMessageService = Executors.newFixedThreadPool(20);

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkEngineServiceImpl.class);
    @Autowired
    private FlinkProcessDao flinkProcessDao;
    @Autowired
    private TaskDao taskDao;
    @Autowired
    private CqlDao cqlDao;
    @Autowired
    private TaskArchiveDao taskArchiveDao;
    @Autowired
    private TaskArchiveVersionDao taskArchiveVersionDao;

    private static FlinkEngineServiceImpl flinkEngineService;

    //初始化静态参数
    @PostConstruct
    public void init() {
        flinkEngineService = this;
    }

    public static Map<TaskPO,FlinkProcessPO> listAllTaskToProcess() {
        Map<TaskPO,FlinkProcessPO> taskToProcessMap = Maps.newHashMap();
        List<TaskPO> taskPOs = flinkEngineService.taskDao.listAll(EngineTypeEnum.FLINK.getValue());
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
        List<FlinkProcessPO> flinkProcessPOs = flinkEngineService.flinkProcessDao.getByIds(params);
        if(CollectionUtils.isEmpty(flinkProcessPOs)){
            return taskToProcessMap;
        }

        for(TaskPO taskPO: taskPOs){
            for(FlinkProcessPO flinkProcessPO: flinkProcessPOs){
               if(taskPO.getProcessId().equals(flinkProcessPO.getId()) && StringUtils.isNotBlank(flinkProcessPO.getYarnAppId())){
                   taskToProcessMap.put(taskPO,flinkProcessPO);
               }
            }
        }
        return taskToProcessMap;
    }

    public static String validateAndBuildTaskConfig(TaskDTO taskDTO) throws Exception {

        FlinkTaskConfigDTO flinkTaskConfigDTO = new FlinkTaskConfigDTO();
        flinkTaskConfigDTO.setTaskName(taskDTO.getTaskName());

        if(taskDTO.getIsCql() == YesOrNoEnum.NO.getValue()){
            if(StringUtils.isBlank(taskDTO.getClassPath())){
                throw new Exception("任务的类路径不能为空！");
            }
            if(StringUtils.isBlank(taskDTO.getArchiveId())){
                throw new Exception("请选择任务运行文件！");
            }
            TaskArchivePO taskArchivePO = flinkEngineService.taskArchiveDao.getTaskArchiveById(Integer.valueOf(taskDTO.getArchiveId()));
            if(taskArchivePO == null ){
                throw new Exception("任务运行文件不存在，或已经被删除！");
            }
            if(taskDTO.getArchiveVersionId()== null){
                throw new Exception("请选择任务运行文件版本！");
            }
            TaskArchiveVersionPO taskArchiveVersionPO = flinkEngineService.taskArchiveVersionDao.getTaskArchiveVersionById(Integer.valueOf(taskDTO.getArchiveVersionId()));
            if(taskArchiveVersionPO == null ){
                throw new Exception("任务运行文件包不存在，或已经被删除！");
            }
            flinkTaskConfigDTO.setProjectJarPath(taskArchiveVersionPO.getTaskArchiveVersionUrl());
            flinkTaskConfigDTO.setClassPath(taskDTO.getClassPath());
        }
        //cql 的情况
        else{
            if(StringUtils.isBlank(taskDTO.getTaskCqlId())|| Integer.valueOf(taskDTO.getTaskCqlId())<=0){
                throw new Exception("请选择CQL脚本！");
            }
            CqlPO cqlPO = flinkEngineService.cqlDao.getCqlById( Integer.valueOf(taskDTO.getTaskCqlId()));
            if(cqlPO == null){
                throw new Exception("选择的CQL脚本不存在或已经被删除！");
            }
            flinkTaskConfigDTO.setClassPath(EngineContant.FLINK_SQL_MAIN_CLASS);
            flinkTaskConfigDTO.setTaskCqlId(cqlPO.getId());
            flinkTaskConfigDTO.setTaskCql(cqlPO.getCqlText());
        }

        //处理其他参数
        if(taskDTO.getWorkerNum()==null||Integer.valueOf(taskDTO.getWorkerNum())<=0){
            throw new Exception("TaskManger 个数不能为空，并且只能为正整数！");
        }
        if(taskDTO.getWorkerMem()==null||Integer.valueOf(taskDTO.getWorkerMem())<=0){
            throw new Exception("每个 TaskManger 分配内存数不能为空，并且只能为正整数！");
        }
        if(Integer.valueOf(taskDTO.getWorkerMem()).intValue() > Integer.valueOf(YarnClientProxy.getConatinerMaxMem())){
            throw new Exception("填写的TaskManger内存数，超过container最大内存数！");
        }
        if(Integer.valueOf(taskDTO.getWorkerMem()).intValue()  < Integer.valueOf(YarnClientProxy.getConatinerMinMem())){
            throw new Exception("填写的TaskManger内存数，小于container最小内存数！");
        }

        flinkTaskConfigDTO.setCustomParams(taskDTO.getCustomParams());
        flinkTaskConfigDTO.setTaskMangerMem(Long.valueOf(taskDTO.getWorkerMem()));
        flinkTaskConfigDTO.setTaskMangerNum(Integer.valueOf(taskDTO.getWorkerNum()));
        flinkTaskConfigDTO.setSlots(Integer.valueOf(taskDTO.getSlots()));
        if(StringUtils.isNumeric(taskDTO.getParallelism())){
            flinkTaskConfigDTO.setParallelism(Integer.valueOf(taskDTO.getParallelism()));
        }
        return JSONObject.toJSONString(flinkTaskConfigDTO);
    }

    public static void beginTask(Integer taskId,String taskConfig) throws Exception{
        if(taskId == null){
            throw new Exception("任务提交失败，参数异常，taskId为空！");
        }
        if(StringUtils.isBlank(taskConfig)){
            throw new Exception("任务提交失败，参数异常，任务配置信息为空！");
        }
        FlinkTaskConfigDTO flinkTaskConfigDTO = JSONObject.parseObject(taskConfig,FlinkTaskConfigDTO.class);
        flinkTaskConfigDTO.setTaskId(taskId);
        if(StringUtils.isBlank(flinkTaskConfigDTO.getTaskName())){
            throw new Exception("任务提交失败，参数异常，任务名为空！");
        }
        EngineContant.FLINK_HOME = ConfigProperty.getConfigValue(ConfigKeyEnum.FLINK_HOME);
        if(StringUtils.isBlank(EngineContant.FLINK_HOME)){
            throw new Exception("任务提交失败，参数异常，系统参数 FLINK_HOME 未进行配置！");
        }
        if(StringUtils.isBlank(ConfigProperty.getConfigValue(ConfigKeyEnum.LOCAL_PROJECT_ITEM_DIR))){
            throw new Exception("任务提交失败，参数异常，系统参数 LOCAL_PROJECT_ITEM_DIR 未进行配置！");
        }
        if(flinkTaskConfigDTO.getTaskMangerNum() == null || flinkTaskConfigDTO.getTaskMangerNum() <=0){
            throw new Exception("任务提交失败，参数异常，taskManager 数必须为正整数！");
        }
        if(flinkTaskConfigDTO.getTaskMangerMem() == null || flinkTaskConfigDTO.getTaskMangerMem()<=0){
            throw new Exception("任务提交失败，参数异常，taskManager 内存必须为正整数！");
        }
        if(flinkTaskConfigDTO.getSlots() == null || flinkTaskConfigDTO.getSlots()<=0){
            throw new Exception("任务提交失败，参数异常，slots 数必须为正整数！");
        }
        if(flinkTaskConfigDTO.getParallelism() != null){
            if(flinkTaskConfigDTO.getParallelism() <=0){
                throw new Exception("任务提交失败，参数异常，Parallelism 数必须为正整数！");
            }
        }
        if(flinkTaskConfigDTO.getTaskMangerMem().longValue() > Integer.valueOf(YarnClientProxy.getConatinerMaxMem())){
            throw new Exception("任务提交失败，TaskManger内存数，超过container最大内存数！");
        }
        if(flinkTaskConfigDTO.getTaskMangerMem().longValue() < Integer.valueOf(YarnClientProxy.getConatinerMinMem())){
            throw new Exception("任务提交失败，TaskManger内存数，小于container最小内存数！");
        }
        //cql情况
        if(flinkTaskConfigDTO.getTaskCqlId()!=null){
            CqlPO cqlPO = flinkEngineService.cqlDao.getCqlById(flinkTaskConfigDTO.getTaskCqlId());
            if(cqlPO == null){
                throw new Exception("选择的CQL脚本不存在或已经被删除！");
            }
            flinkTaskConfigDTO.setTaskCql(cqlPO.getCqlText());

            String CQL_ENGINE_PROJECT_NAME = ConfigProperty.getConfigValue(ConfigKeyEnum.CQL_ENGINE_PROJECT_NAME);
            if(StringUtils.isBlank(CQL_ENGINE_PROJECT_NAME)){
                throw new Exception("任务提交失败，参数异常，系统参数 CQL_ENGINE_PROJECT_NAME 未进行配置！");
            }
            String cqlEngineUrl = flinkEngineService.taskArchiveVersionDao.getCqlEngineUrl(CQL_ENGINE_PROJECT_NAME);
            if(StringUtils.isBlank(cqlEngineUrl)){
                throw new Exception("任务提交失败，未初始化CQL任务引擎！");
            }
            flinkTaskConfigDTO.setProjectJarPath(cqlEngineUrl);
            flinkTaskConfigDTO.setCustomParams(FlinkSqlUtils.getBase64(flinkTaskConfigDTO.getTaskCql()));
        }
        EngineBusiness.pendingTask(flinkTaskConfigDTO.getTaskName(),flinkTaskConfigDTO);
    }

    public static boolean taskSubmit(FlinkTaskConfigDTO flinkTaskConfigDTO , TaskSubmitTypeEnum submitTypeEnum) {
        Integer taskId = flinkTaskConfigDTO.getTaskId();
        TaskStartTimeLineDTO taskStartTimeLineDTO = null;
        String appSubmitLogMessage = "";
        String jobSubmitLogMessage = "";
        String jobId = "";
        String yarnAppId = "";
        String jobmanagerAddress = "";
        Process startYarnSessionPr = null;
        Process startJobPr = null;
        Integer processId = null;
        try {
            taskStartTimeLineDTO = new TaskStartTimeLineDTO(flinkTaskConfigDTO.getTaskName(),"准备提交任务，正在进行预处理");
            EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO);

            // 获得jobmeneager默认内存数。不设置默认为2048m
            String JOB_MANGER_DEFAULT_MEM = ConfigProperty.getConfigValue(ConfigKeyEnum.FLINK_JOB_MANGER_MEM);
            flinkTaskConfigDTO.setJobMangerMem(EngineContant.JOB_MANGER_DEFAULT_MEM);
            if(StringUtils.isNotBlank(JOB_MANGER_DEFAULT_MEM)){
                flinkTaskConfigDTO.setJobMangerMem(Long.valueOf(JOB_MANGER_DEFAULT_MEM));
            }

            // 获得超时时长
            String jogSubmitTimeOut = ConfigProperty.getConfigValue(ConfigKeyEnum.FLINK_JOB_START_TIMEOUT);
            if(!StringUtils.trimToEmpty(jogSubmitTimeOut).equals("")){
                jogSubmitTimeOut = "60";;
            }

            // 保存提交历史
            FlinkProcessPO flinkProcessPO = new FlinkProcessPO();
            flinkProcessPO.setTaskId(taskId);
            flinkProcessPO.setStartTime(new Date());
            flinkProcessPO.setSubmitType(submitTypeEnum.getValue());
            flinkProcessPO.setTaskConfig(JSONObject.toJSONString(flinkTaskConfigDTO));
            flinkEngineService.flinkProcessDao.insert(flinkProcessPO);
            processId = flinkProcessPO.getId();

            // 开始session 输出时间线
            EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_OK, "准备提交任务，正在启动yarn-session"));

            //------------------------------------------starYarnSession开始------------------------------------//

            // 开始session
            startYarnSessionPr = FlinkOnYarnBusiness.startYarnSession(flinkTaskConfigDTO);
            if(startYarnSessionPr == null){
                throw new Exception("startYarnSession failed!");
            }

            BufferedReader messageStream = new BufferedReader(new InputStreamReader(startYarnSessionPr.getInputStream()));
            Future<String> debugMessageFuture  = printMessageService.submit(new PrintStartYarnSessionThread(messageStream , flinkTaskConfigDTO.getTaskName(),flinkProcessPO));

            // 验证session是否开始成功
            try{
                appSubmitLogMessage = debugMessageFuture.get(START_SESSION_MAX_WAIT,TimeUnit.SECONDS);
            }catch (Exception e){
            }
            yarnAppId = flinkProcessPO.getYarnAppId();
            if(StringUtils.isBlank(appSubmitLogMessage)){
                throw new Exception("startYarnSession time out!");
            }

            //更新日志
            flinkProcessPO.setJobLogMessage(appSubmitLogMessage);
            flinkEngineService.flinkProcessDao.update(flinkProcessPO);

            //检查是否提交成功
            if(!StringUtils.contains(appSubmitLogMessage,LOG_submittedAppSuccess)){
                throw new Exception("startYarnSession failed!");
            }

            //------------------------------------------starYarnSession结束------------------------------------//

            //重新获得一次，因为 startYarnSession 线程内部重新设置了对象
            taskStartTimeLineDTO = EngineBusiness.getTaskStartTimeLine(flinkTaskConfigDTO.getTaskName());

            // 开始job 输出时间线
            EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_OK, "正在向flink集群提交job程序"));

            //------------------------------------------startJob开始------------------------------------//

            // 开始job
            startJobPr = FlinkOnYarnBusiness.startJob(flinkTaskConfigDTO,yarnAppId);
            if(startJobPr==null){
                throw new Exception("startJob failed!");
            }

            BufferedReader startJobMessageStream = new BufferedReader(new InputStreamReader(startJobPr.getInputStream()));
            Future<String> debugMessageFuture1  = printMessageService.submit(new PrintMessageThread(startJobMessageStream,jogSubmitTimeOut));

            // 验证job是否开始成功
            try{
                jobSubmitLogMessage = debugMessageFuture1.get(Integer.valueOf(jogSubmitTimeOut),TimeUnit.SECONDS);
            }catch (Exception e){
            }
            if(StringUtils.isBlank(jobSubmitLogMessage)){
                throw new Exception("startJob time out!");
            }

            // 更新日志
            flinkProcessPO.setJobLogMessage(appSubmitLogMessage + jobSubmitLogMessage);
            flinkEngineService.flinkProcessDao.update(flinkProcessPO);

            // 从日志中截取jobId
            jobId = StringUtils.substringBetween(jobSubmitLogMessage,LOG_submittedJob,EngineContant.LOG_spliter);
            if(StringUtils.isBlank(jobId)){
                throw new Exception("startJob failed!");
            }

            // 更新jobId
            flinkProcessPO.setJobId(jobId);
            flinkEngineService.flinkProcessDao.update(flinkProcessPO);

            //------------------------------------------startJob结束------------------------------------//

            // 开始验证jobRUNNING
            EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_OK, "正在检查job程序是否RUNNING"));

            boolean startJobFailed = false;
            FlinkJobDTO flinkJobDTO = FlinkWebClientBusiness.getFlinkJobDtoWithRetry(yarnAppId,jobId);
            if(flinkJobDTO == null){
                startJobFailed = true;
            }

            // 如果不是RUNNING或者FINISHED任务 开始RUNNIN失败
            String jobLogMessage = "";
            if(flinkJobDTO == null || (!(flinkJobDTO.getState().equals("RUNNING") || flinkJobDTO.getState().equals("FINISHED")))){
                // 先更新exception信息
                String jobRootException = FlinkWebClientBusiness.getJobRootException(yarnAppId,jobId);
                if(StringUtils.isNotBlank(jobRootException)){
                    jobLogMessage = StringUtils.replace(jobRootException, "\n\t",EngineContant.LOG_spliter);
                    jobLogMessage = jobLogMessage + EngineContant.LOG_spliter;
                    flinkProcessPO.setJobLogMessage(flinkProcessPO.getJobLogMessage() + jobLogMessage);
                    flinkEngineService.flinkProcessDao.update(flinkProcessPO);
                }
                throw new Exception("startJob failed：" + flinkProcessPO.getJobLogMessage());
            }

            // 最后再查一遍，避免数据已经被删除
            TaskPO taskPO = flinkEngineService.taskDao.getById(taskId);
            if(taskPO == null){
                throw new Exception("task is deleted !!!");
            }

            //更新提交成功
            flinkEngineService.flinkProcessDao.updateSubmitOk(processId);

            //更新任务状态
            taskPO.setTaskStatus(TaskStatusEnum.RUNNING.getValue());
            taskPO.setTaskStartTime(new Date());
            taskPO.setProcessId(processId);
            taskPO.setId(taskId);
            flinkEngineService.taskDao.update4Start(taskPO);

            EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToEnd(EngineContant.TIMELINE_ITEM_OK,"任务开始成功！", EngineContant.TIMELINE_OK));

            //提交成功先保存一次信息
            FlinkEngineServiceImpl.processTaskSavepoint(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId(),FlinkJobSavePointEnum.COMMON.getValue(),flinkProcessPO.getId());
            return true;
        } catch (Throwable e) {
            LOGGER.error("FlinkEngineServiceImpl taskSubmit is error",e);
            try{
                if(startYarnSessionPr!=null){
                    BufferedReader startYarnSessionErrorStream = new BufferedReader(new InputStreamReader(startYarnSessionPr.getErrorStream()));
                    printMessageService.submit(new PrintErrorThread(startYarnSessionErrorStream,processId));
                }
                if(startJobPr!=null){
                    BufferedReader startJobErrorStream = new BufferedReader(new InputStreamReader(startJobPr.getErrorStream()));
                    printMessageService.submit(new PrintErrorThread(startJobErrorStream,processId));
                }

                //重新获得一次，因为 startYarnSession 线程内部重新设置了对象
                taskStartTimeLineDTO = EngineBusiness.getTaskStartTimeLine(flinkTaskConfigDTO.getTaskName());

                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_FAILD,"提交flink任务失败，正在进行回滚"));

                //等待30秒记录错误日志，防止kill了app以后输出不了日志
                TimeUnit.SECONDS.sleep(30);

                FlinkOnYarnBusiness.stopJob(yarnAppId,jobId);
                ApplicationReport rpplicationReport = YarnClientProxy.getApplicationReportByAppId(yarnAppId);
                if(rpplicationReport !=null){
                    LOGGER.error("FlinkEngineServiceImpl rollbackTaskSubmit kill appID =" + yarnAppId);
                    YarnClientProxy.killApplicationByAppId(yarnAppId);
                }

                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToEnd(EngineContant.TIMELINE_ITEM_OK, "任务开始失败！", EngineContant.TIMELINE_FAILD));
            }catch(Exception e1){
                EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToEnd(EngineContant.TIMELINE_ITEM_FAILD, "任务开始失败！", EngineContant.TIMELINE_FAILD));
                LOGGER.error("FlinkEngineServiceImpl rollbackTaskSubmit is error",e1);
            }
            return false;
        }finally {
            if(startYarnSessionPr!=null){
                startYarnSessionPr.destroy();
            }
            if(startJobPr!=null){
                startJobPr.destroy();
            }
        }
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
        FlinkProcessPO flinkProcessPO = flinkEngineService.flinkProcessDao.getById(processId);
        if(flinkProcessPO == null){
            throw new Exception("任务停止失败，任务的执行信息不存在或者已经被删除！");
        }
        EngineContant.FLINK_HOME = ConfigProperty.getConfigValue(ConfigKeyEnum.FLINK_HOME);
        if(StringUtils.isBlank(EngineContant.FLINK_HOME)){
            throw new Exception("任务停止失败，参数异常，系统参数 FLINK_HOME 未进行配置！");
        }
        if(!FlinkOnYarnBusiness.stopJob(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId())){
            throw new Exception("任务停止失败！");
        }

        ApplicationReport report = YarnClientProxy.getApplicationReportByAppId(flinkProcessPO.getYarnAppId());
        if(report != null){
            YarnApplicationState state = report.getYarnApplicationState();
            LOGGER.error("FlinkEngineServiceImpl stopTask report YarnApplicationState = " + state.toString() + " appId=" + flinkProcessPO.getYarnAppId());
            if (!(YarnApplicationState.FINISHED == state || YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state)) {
                try{
                    YarnClientProxy.killApplicationByAppId(flinkProcessPO.getYarnAppId());
                    LOGGER.error("FlinkEngineServiceImpl stopTask killApplicationByAppId is ok appId=" + flinkProcessPO.getYarnAppId());
                }catch (Exception e){
                    LOGGER.error("FlinkEngineServiceImpl stopTask killApplicationByAppId is error appId=" + flinkProcessPO.getYarnAppId(),e);
                    throw e;
                }
            }
        }

        TaskPO taskPO = new TaskPO();
        taskPO.setTaskStatus(TaskStatusEnum.STOP.getValue());
        taskPO.setTaskStopTime(new Date());
        taskPO.setId(flinkProcessPO.getTaskId());
        flinkEngineService.taskDao.update4Stop(taskPO);

        FlinkEngineCheckPointImpl.recoveryFail.remove(flinkProcessPO.getTaskId());
    }

    public static void fillTaskConfigToDto(TaskDTO taskDTO, String taskConfig,boolean withShow) {
        FlinkTaskConfigDTO flinkTaskConfigDTO = JSONObject.parseObject(taskConfig, FlinkTaskConfigDTO.class);
        taskDTO.setWorkerMem(flinkTaskConfigDTO.getTaskMangerMem().toString());
        taskDTO.setWorkerNum(flinkTaskConfigDTO.getTaskMangerNum().toString());
        taskDTO.setClassPath(flinkTaskConfigDTO.getClassPath());
        taskDTO.setTaskCql(flinkTaskConfigDTO.getTaskCql());
        taskDTO.setTaskCqlId(flinkTaskConfigDTO.getTaskCqlId()==null?"":flinkTaskConfigDTO.getTaskCqlId().toString());
        taskDTO.setCustomParams(flinkTaskConfigDTO.getCustomParams());
        taskDTO.setSlots(flinkTaskConfigDTO.getSlots()==null?"":flinkTaskConfigDTO.getSlots().toString());
        taskDTO.setParallelism(flinkTaskConfigDTO.getParallelism()==null?"":flinkTaskConfigDTO.getParallelism().toString());
        //填充详细信息
        if(withShow){
            taskDTO.setYarnAddress("");
            if(taskDTO.getProcessId()!=null && taskDTO.getProcessId()>=0 ){
                FlinkProcessPO flinkProcessPO = flinkEngineService.flinkProcessDao.getById(taskDTO.getProcessId());
                if(StringUtils.isNotBlank(flinkProcessPO.getYarnAppId())){
                    taskDTO.setYarnAddress(FlinkWebClientBusiness.buildYarnProxyAddress(flinkProcessPO.getYarnAppId()) + "/#/overview");
                }
            }
            if(StringUtils.isNotBlank(taskDTO.getTaskCqlId())){
                CqlPO cqlPO = flinkEngineService.cqlDao.getCqlById(Integer.valueOf(taskDTO.getTaskCqlId()));
                if(cqlPO != null ){
                    String remark = StringUtils.isNotBlank(cqlPO.getCqlRemark())?" [" + cqlPO.getCqlRemark() +"]":"";
                    taskDTO.setTaskCqlShow(cqlPO.getCqlName() + remark);
                }
            }
        }
    }

    public static void processTaskSavepoint(String yarnAppId, String jobId, Integer savepointType, Integer processId) {
        try{
            if(FlinkJobSavePointEnum.COMMON.getValue() == savepointType){
                //直接更新
                FlinkProcessPO updateProcessPO = new FlinkProcessPO();
                updateProcessPO.setId(processId);

                FlinkTaskDetailDTO flinkTaskDetailDTO = new FlinkTaskDetailDTO();
                flinkTaskDetailDTO.setAppOverview(FlinkWebClientBusiness.getYarnAppOveriew(yarnAppId));
                flinkTaskDetailDTO.setExceptions(FlinkWebClientBusiness.getJobException(yarnAppId,jobId));
                flinkTaskDetailDTO.setJobDetail(FlinkWebClientBusiness.getJobDetail(yarnAppId,jobId));
                flinkTaskDetailDTO.setJobmanagerConfig(FlinkWebClientBusiness.getJobManagerConfig(yarnAppId));
                flinkTaskDetailDTO.setTaskManagers(FlinkWebClientBusiness.getTaskManagerDetail(yarnAppId));
                flinkTaskDetailDTO.setVertices(FlinkWebClientBusiness.getVerticeDetail(yarnAppId,jobId));
                flinkTaskDetailDTO.setSavepointType(savepointType);
                flinkTaskDetailDTO.setSavepointTime(DateUtil.getCurrentDateTime());

                //再拿一次如果能拿到就更新 防止忽然程序挂的的情况，把以前的详情盖掉丢失更新
                if(StringUtils.isNotBlank(FlinkWebClientBusiness.getJobDetail(yarnAppId,jobId))){
                    updateProcessPO.setTaskDetail(JSONObject.toJSONString(flinkTaskDetailDTO));
                    flinkEngineService.flinkProcessDao.update(updateProcessPO);
                }
            }else{
                //只更新 savepointType
                FlinkProcessPO flinkProcessPO = flinkEngineService.flinkProcessDao.getById(processId);
                FlinkTaskDetailDTO flinkTaskDetailDTO = JSONObject.parseObject(flinkProcessPO.getTaskDetail(), FlinkTaskDetailDTO.class);
                if(flinkTaskDetailDTO!=null){

                    FlinkProcessPO updateProcessPO = new FlinkProcessPO();
                    updateProcessPO.setId(processId);

                    flinkTaskDetailDTO.setSavepointType(savepointType);
                    flinkTaskDetailDTO.setSavepointTime(DateUtil.getCurrentDateTime());
                    updateProcessPO.setTaskDetail(JSONObject.toJSONString(flinkTaskDetailDTO));
                    flinkEngineService.flinkProcessDao.update(updateProcessPO);
                }
            }
        }catch(Exception e){
            LOGGER.error("FlinkEngineServiceImpl processTaskSavepoint is error",e);
        }
    }

    /**
     * 打印开始 yarnsession的信息的线程。需返回日志信息
     */
    private static class PrintStartYarnSessionThread implements Callable<String> {
        private Integer maxClusterTimeOut = 10;
        private BufferedReader messageStreambf;
        private String taskName;
        private FlinkProcessPO flinkProcessPO;

        public PrintStartYarnSessionThread(BufferedReader messageStreambf, String taskName, FlinkProcessPO flinkProcessPO) {
            this.messageStreambf = messageStreambf;
            this.taskName = taskName;
            this.flinkProcessPO = flinkProcessPO;
        }

        public String call() throws Exception {
            StringBuilder startYarnSessionMessage = new StringBuilder();
            try {
                long begin = System.currentTimeMillis();
                TaskStartTimeLineDTO taskStartTimeLineDTO = EngineBusiness.getTaskStartTimeLine(taskName);

                while((((System.currentTimeMillis() - begin) /1000) < (START_SESSION_MAX_WAIT + 10))){
                    String line=messageStreambf.readLine();
                    if(line == null){
                        break;
                    }
                    LOGGER.error("debug Info: " + line);
                    startYarnSessionMessage.append(line);
                    startYarnSessionMessage.append(EngineContant.LOG_spliter);

                    if(StringUtils.contains(line,LOG_deployCLusterTimeOut)){
                        maxClusterTimeOut --;
                    }else if(StringUtils.contains(line,LOG_submitingApp) && taskStartTimeLineDTO != null){
                        EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_OK,"正在向yarn环境提交application master"));
                    }else if(StringUtils.contains(line,LOG_submittedApp) && taskStartTimeLineDTO != null){

                        String yarnAppId = StringUtils.substringBetween(startYarnSessionMessage.toString(),LOG_submittedApp,EngineContant.LOG_spliter);
                        flinkProcessPO.setYarnAppId(yarnAppId);
                        flinkEngineService.flinkProcessDao.update(flinkProcessPO);

                    }else if(StringUtils.contains(line,LOG_deployingCLuster) && taskStartTimeLineDTO != null){
                        EngineBusiness.sysTaskStartTimeLine(taskStartTimeLineDTO.goToNext(EngineContant.TIMELINE_ITEM_OK,"正在部署flink集群环境"));
                    }

                    //最大集群超时部署错误的次数
                    if(maxClusterTimeOut<=0){
                        break;
                    }
                }
            }catch (Throwable e){
                LOGGER.error("FlinkEngineServiceImpl PrintStartYarnSessionThread is complate");
            }finally {
                if(messageStreambf!=null){
                    try {
                        messageStreambf.close();
                    }catch (Exception e){
                    }
                    messageStreambf = null;
                }
            }
            return startYarnSessionMessage.toString();
        }
    }

    /**
     * 打印message消息流
     */
    private static class PrintMessageThread implements Callable<String> {
        private BufferedReader messageStreambf;

        public PrintMessageThread(BufferedReader messageStreambf,String jogSubmitTimeOut) {
            this.messageStreambf = messageStreambf;
        }

        public String call() throws Exception {
            StringBuilder message=new StringBuilder();
            try {
                long begin = System.currentTimeMillis();
                while((((System.currentTimeMillis() - begin) /1000) < (START_SESSION_MAX_WAIT + 10))){
                    String line=messageStreambf.readLine();
                    if(line == null){
                        break;
                    }
                    LOGGER.error("debug Info: " + line);
                    message.append(line);
                    message.append(EngineContant.LOG_spliter);
                }
            }catch (Throwable e){
                LOGGER.error("FlinkEngineServiceImpl PrintMessageThread is complate");
            }finally {
                if(messageStreambf!=null){
                    try {
                        messageStreambf.close();
                    }catch (Exception e){
                    }
                }
                messageStreambf = null;
            }
            return message.toString();
        }
    }

    /**
     * 打印错误消息流
     */
    private static class PrintErrorThread implements Runnable {
        private BufferedReader errorStreambf;
        private Integer processId;

        public PrintErrorThread(BufferedReader errorStreambf,Integer processId) {
            this.errorStreambf = errorStreambf;
            this.processId = processId;
        }

        public void run()  {
            StringBuilder message=new StringBuilder();
            try {
                String line;
                while((line=errorStreambf.readLine())!= null){
                    LOGGER.error("error Info: " + line);
                    message.append(line);
                    message.append(EngineContant.LOG_spliter);
                }
                if(processId!=null && StringUtils.isNotBlank(message.toString())){
                    FlinkProcessPO flinkProcessPO = flinkEngineService.flinkProcessDao.getById(processId);
                    if(flinkProcessPO!=null && StringUtils.isNotBlank(message.toString())){
                        flinkProcessPO.setJobLogMessage(flinkProcessPO.getJobLogMessage() + message.toString());
                        flinkEngineService.flinkProcessDao.update(flinkProcessPO);
                    }
                }
            }catch (Throwable e){
                LOGGER.error("FlinkEngineServiceImpl PrintErrorThread is complate");
            }finally {
                if(errorStreambf!=null){
                    try {
                        errorStreambf.close();
                    }catch (Exception e){
                    }
                }
                errorStreambf = null;
            }
        }
    }
}
