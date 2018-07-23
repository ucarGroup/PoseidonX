package com.ucar.streamsuite.engine.business;

import com.alibaba.fastjson.JSONObject;
import com.ucar.streamsuite.common.constant.*;
import com.ucar.streamsuite.common.util.DistributedReentrantLock;
import com.ucar.streamsuite.common.util.ZKUtil;
import com.ucar.streamsuite.engine.constants.EngineContant;
import com.ucar.streamsuite.engine.dto.FlinkTaskConfigDTO;
import com.ucar.streamsuite.engine.dto.JstormTaskConfigDTO;
import com.ucar.streamsuite.engine.dto.TaskConfigDTO;
import com.ucar.streamsuite.engine.service.impl.FlinkEngineServiceImpl;
import com.ucar.streamsuite.engine.service.impl.JstormEngineServiceImpl;
import com.ucar.streamsuite.task.dto.TaskDTO;
import com.ucar.streamsuite.task.dto.TaskStartTimeLineDTO;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Description: 引擎入口业务类
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class EngineBusiness {

    public static final Logger LOGGER = LoggerFactory.getLogger(EngineBusiness.class);

    /**
     * 开始任务
     * @param taskConfig
     * @throws Exception
     */
    public static void beginTask(Integer taskId, String taskConfig, Integer engineType) throws Exception{
        if(engineType== EngineTypeEnum.JSTORM.getValue()){
            JstormEngineServiceImpl.beginTask(taskId,taskConfig);
        }
        if(engineType== EngineTypeEnum.FLINK.getValue()){
            FlinkEngineServiceImpl.beginTask(taskId,taskConfig);
        }
    }

    /**
     * 停止任务
     * @param taskId
     * @param taskName
     * @param processId
     * @throws Exception
     */
    public static void stopTask(Integer taskId, String taskName, Integer processId, Integer engineType) throws Exception{
        DistributedReentrantLock lock = null;
        try {
            CuratorFramework cfClient = ZKUtil.getClient(StreamContant.HDFS_HADOOP_ZOOKEEPER);
            lock = new DistributedReentrantLock(cfClient, EngineContant.TASK_STOP_LOCK_PRE + taskName);
            boolean islock = lock.tryLock(3, TimeUnit.SECONDS);
            if(!islock){
                throw new Exception("任务停止失败，任务正在被其他用户停止，请稍后重试！");
            }
            if(engineType == EngineTypeEnum.JSTORM.getValue()){
                JstormEngineServiceImpl.stopTask(taskId,taskName,processId);
            }
            if(engineType == EngineTypeEnum.FLINK.getValue()){
                FlinkEngineServiceImpl.stopTask(taskId,taskName,processId);
            }
        } catch (Exception e) {
            LOGGER.error("stopTask is error",e);
            if (lock != null) {
                try {
                    lock.unlock();
                } catch (Exception e1) {
                }
            }
            throw e;
        } finally {
            if (lock != null) {
                try {
                    lock.unlock();
                } catch (Exception e1) {
                }
            }
        }
    }

    /**
     * 校验任务信息并构建taskConfig信息
     * @param taskDTO
     * @return
     * @throws Exception
     */
    public static String validateAndBuildTaskConfig(TaskDTO taskDTO, Integer engineType) throws Exception{
        String config = "";
        if(engineType == EngineTypeEnum.JSTORM.getValue()){
            config = JstormEngineServiceImpl.validateAndBuildTaskConfig(taskDTO);
        }
        if(engineType  == EngineTypeEnum.FLINK.getValue()) {
            config = FlinkEngineServiceImpl.validateAndBuildTaskConfig(taskDTO);
        }
        return config;
    }

    /**
     * 用taskConfig的信息填充taskDto
     * @param taskDTO
     * @param taskConfig
     * @param withShow
     */
    public static void fillTaskConfigToDto(TaskDTO taskDTO, String taskConfig, boolean withShow, Integer engineType){
        if(engineType == EngineTypeEnum.JSTORM.getValue()){
            JstormEngineServiceImpl.fillTaskConfigToDto(taskDTO, taskConfig ,withShow);
        }
        if(engineType == EngineTypeEnum.FLINK.getValue()){
            FlinkEngineServiceImpl.fillTaskConfigToDto(taskDTO, taskConfig, withShow);
        }
    }

    /**
     * 提交
     * @return
     */
    public static void pendingTask(String taskName,TaskConfigDTO taskConfigDTO) throws Exception{
        TaskStartTimeLineDTO taskStartTimeLineDTO = new TaskStartTimeLineDTO(taskName,"");
        sysTaskStartTimeLine(taskStartTimeLineDTO);
        Submitter.pending(taskName,taskConfigDTO);
    }

    /**
     * 同步任务开始时间线数据
     * @param taskStartTimeLineDTO
     */
    public static void sysTaskStartTimeLine(TaskStartTimeLineDTO taskStartTimeLineDTO){
        try{
            CuratorFramework cfClient = ZKUtil.getClient(StreamContant.HDFS_HADOOP_ZOOKEEPER);
            // 存储ZK
            if(!ZKUtil.pathExsit(cfClient, EngineContant.TASK_SUBMIT_TIMELINE_PRE + taskStartTimeLineDTO.getTaskName())){
                ZKUtil.createPath(cfClient, EngineContant.TASK_SUBMIT_TIMELINE_PRE + taskStartTimeLineDTO.getTaskName(), JSONObject.toJSONString(taskStartTimeLineDTO));
            }else{
                ZKUtil.setData(cfClient, EngineContant.TASK_SUBMIT_TIMELINE_PRE + taskStartTimeLineDTO.getTaskName(),JSONObject.toJSONString(taskStartTimeLineDTO));
            }
        }catch(Exception e){
            LOGGER.error("FlinkOnYarnBusiness sysTaskStartTimeLine is error",e);
        }
    }

    /**
     * 获得任务开始时间线数据
     */
    public static TaskStartTimeLineDTO getTaskStartTimeLine(String taskName){
        TaskStartTimeLineDTO taskStartTimeLineDTO = null;
        try{
            CuratorFramework cfClient = ZKUtil.getClient(StreamContant.HDFS_HADOOP_ZOOKEEPER);
            byte[] object = ZKUtil.getData(cfClient, EngineContant.TASK_SUBMIT_TIMELINE_PRE + taskName);
            if(object!= null){
                taskStartTimeLineDTO = JSONObject.parseObject(new String(object), TaskStartTimeLineDTO.class);
            }
        }catch(Exception e){
            LOGGER.error("FlinkOnYarnBusiness getTaskStartTimeLine is error",e);
        }
        return taskStartTimeLineDTO;
    }

    /**
     * Description: 提交者
     * Created on 2018/1/18 下午4:33
     *
     *
     */
    public static class Submitter {
        private static final Logger LOGGER = LoggerFactory.getLogger(Submitter.class);

        private static final LinkedBlockingQueue<String> pendingQueue = new LinkedBlockingQueue<String>(500);

        //防止分布式锁判定失误的时候一个任务多次执行。反复提交。提交者按顺序提交不同任务名字的任务。
        private static ConcurrentHashMap<String,TaskConfigDTO> taskToConfigSubmitting = new ConcurrentHashMap<String,TaskConfigDTO>();

        static{
            pendingQueue.clear();
            taskToConfigSubmitting.clear();
            Thread topSubmitterThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        String taskName = "";
                        try {
                            TimeUnit.SECONDS.sleep(1);

                            taskName = pendingQueue.poll(3, TimeUnit.SECONDS);
                            if (StringUtils.isBlank(taskName)) {
                                continue;
                            }

                            if(taskToConfigSubmitting.containsKey(taskName)){
                                taskSubmit(taskName, taskToConfigSubmitting.get(taskName),TaskSubmitTypeEnum.SUBMIT);
                                taskToConfigSubmitting.remove(taskName);
                            }

                        } catch (Throwable e) {
                            if(StringUtils.isNotBlank(taskName)){
                                taskToConfigSubmitting.remove(taskName);
                            }
                            LOGGER.error("topSubmitterThread is error", e);
                        }
                    }
                }
            });
            topSubmitterThread.setDaemon(true);
            topSubmitterThread.start();
        }

        /**
         * 拓扑提交，用任务名锁分布式锁，
         * @return
         */
        public static boolean taskSubmit(String taskName,TaskConfigDTO taskConfigDTO, TaskSubmitTypeEnum submitTypeEnum) {
            // 创建时间线（用于页面显示，滚动效果）
            boolean isOk = false;
            DistributedReentrantLock lock = null;
            try {
                CuratorFramework cfClient = ZKUtil.getClient(StreamContant.HDFS_HADOOP_ZOOKEEPER);
                lock = new DistributedReentrantLock(cfClient, EngineContant.TASK_SUBMIT_LOCK_PRE + taskName);
                // 3秒都无法获得锁认为false
                boolean islock = lock.tryLock(3, TimeUnit.SECONDS);
                if(islock){
                    //如果是jstorm则进行jstorm任务的提交
                    if(taskConfigDTO.getClass() == JstormTaskConfigDTO.class){
                        JstormTaskConfigDTO jstormTaskConfigDTO = (JstormTaskConfigDTO)taskConfigDTO;
                        isOk = JstormEngineServiceImpl.taskSubmit(jstormTaskConfigDTO, submitTypeEnum);
                    }
                    if(taskConfigDTO.getClass() == FlinkTaskConfigDTO.class){
                        FlinkTaskConfigDTO flinkTaskConfigDTO = (FlinkTaskConfigDTO)taskConfigDTO;
                        isOk = FlinkEngineServiceImpl.taskSubmit(flinkTaskConfigDTO, submitTypeEnum);
                    }
                }else{
                    throw new Exception("服务器正忙。。。请稍后重新提交！");
                }
                return isOk;
            } catch (Throwable e) {
                LOGGER.error("taskSubmit is error",e);
                if (lock != null) {
                    try {
                        lock.unlock();
                    } catch (Exception e1) {
                    }
                }
                return isOk;
            } finally {
                if (lock != null) {
                    try {
                        lock.unlock();
                    } catch (Exception e1) {
                    }
                }
            }
        }

        /**
         * 提交任务到队列
         * @return
         */
        public static void pending(String taskName,TaskConfigDTO taskConfigDTO) throws Exception{
            CuratorFramework cfClient = ZKUtil.getClient(StreamContant.HDFS_HADOOP_ZOOKEEPER);
            if(CollectionUtils.isNotEmpty(ZKUtil.getChildrens(cfClient, EngineContant.TASK_SUBMIT_LOCK_PRE + taskName))){
                throw new Exception("任务提交失败，("+ taskName +") 任务正在提交中，尚未处理完毕，请稍后重新提交！");
            }
            if(taskToConfigSubmitting.putIfAbsent(taskName,taskConfigDTO) == null){
                if (!pendingQueue.offer(taskName, 1, TimeUnit.SECONDS)) {
                    taskToConfigSubmitting.remove(taskName);
                    throw new Exception("服务器过于繁忙，请稍后重新提交！");
                }
            }else{
                throw new Exception("任务提交失败，("+ taskName +") 任务正在提交中，尚未处理完毕，请稍后重新提交！");
            }
        }
    }

}
