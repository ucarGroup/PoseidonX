package com.ucar.streamsuite.moniter.service.impl;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;
import com.alibaba.fastjson.JSONObject;

import com.alibaba.fastjson.TypeReference;
import com.alibaba.jstorm.metric.MetricDef;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ucar.streamsuite.common.constant.EngineTypeEnum;
import com.ucar.streamsuite.common.constant.JstormWorkerErrorEnum;
import com.ucar.streamsuite.common.constant.TaskStatusEnum;
import com.ucar.streamsuite.common.hbase.CommonHbaseRecord;
import com.ucar.streamsuite.common.hbase.util.HBaseRecordUtils;
import com.ucar.streamsuite.common.hbase.util.HBaseUtils;
import com.ucar.streamsuite.common.util.DateUtil;
import com.ucar.streamsuite.common.util.LeaderSelecter;
import com.ucar.streamsuite.dao.mysql.JstormProcessDao;
import com.ucar.streamsuite.dao.mysql.TaskDao;
import com.ucar.streamsuite.engine.business.JStormClusterBusiness;

import com.ucar.streamsuite.engine.business.JStormTopologyBusiness;
import com.ucar.streamsuite.engine.constants.EngineContant;
import com.ucar.streamsuite.engine.dto.JstormCompenentTaskDTO;
import com.ucar.streamsuite.engine.dto.JstormTaskConfigDTO;
import com.ucar.streamsuite.engine.po.JstormProcessPO;
import com.ucar.streamsuite.engine.service.impl.JstormEngineServiceImpl;
import com.ucar.streamsuite.moniter.business.impl.JstormMetricBusiness;
import com.ucar.streamsuite.moniter.business.impl.MetricReportContainer;
import com.ucar.streamsuite.moniter.constants.MoniterContant;
import com.ucar.streamsuite.moniter.dto.*;
import com.ucar.streamsuite.task.po.TaskPO;

import org.apache.commons.collections.CollectionUtils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import shade.storm.org.apache.thrift.TException;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * Description: jstorm 引擎监控指标收集服务类
 * Created on 2018/1/30 上午10:59
 *
 *
 */
@Component
public class JstormMetricCollectImpl {

    //jstorm指标
    private static final String METRIC_DEF_THREAD_STATUS= "thread_status";
    private static final String METRIC_WORKER_GC_COUNT = "GCCount";
    private static final String METRIC_WORKER_GC_TIME = "GCTime";

    private static final Logger LOGGER = LoggerFactory.getLogger(JstormMetricCollectImpl.class);

    private static final ScheduledExecutorService metricCollector = Executors.newScheduledThreadPool(1);

    //使用的线程池个数较大 要保证规定时间内执行完
    private static final ExecutorService metricCollectExecutorService = Executors.newFixedThreadPool(20);

    private static JstormMetricCollectImpl metricCollectImpl;

    //存放 top 的worker 出错历史缓存。（为了避免疯狂插入数据），需要在停止任务的时候清除缓存，防止内存泄露
    public static ConcurrentHashMap<String,HashMap<String,Integer>>  workerErrorHistoryCache = new ConcurrentHashMap<String,HashMap<String,Integer>>();

    @Autowired
    private TaskDao taskDao;
    @Autowired
    private JstormProcessDao jstormProcessDao;

    static{
        startMetricCollector();
    }

    //初始化静态参数
    @PostConstruct
    public void init() {
        metricCollectImpl = this;
    }

    /**
     * 开始指标收集线程
     */
    private static void startMetricCollector(){
        /**
         * METRIC_COLLECT_CYCLE 秒执行一次
         */
        metricCollector.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    //long begin = System.currentTimeMillis();

                    Set<Future> futures = Sets.newHashSet();
                    Map<TaskPO,JstormProcessPO> taskToProcessMap = JstormEngineServiceImpl.listAllTaskToProcess();
                    for(Map.Entry<TaskPO,JstormProcessPO> taskToProcess:taskToProcessMap.entrySet()){
                        Future future = metricCollectExecutorService.submit(new JstormMetricCollectExecutor( taskToProcess.getKey(),taskToProcess.getValue()));
                        futures.add(future);
                    }
                    for(Future future: futures){
                        try {
                            //最大等待55秒防止1分钟执行不完
                            future.get(MoniterContant.METRIC_COLLECT_CYCLE - 5, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            future.cancel(true);
                        }
                    }
                    //LOGGER.error("JstormMetricCollectImpl startMetricCollector end" + " exeTime = " + (System.currentTimeMillis() - begin) /1000);
                } catch (Throwable e) {
                    LOGGER.error("JstormMetricCollectImpl startMetricCollector is error", e);
                }
            }
        }, 1, MoniterContant.METRIC_COLLECT_CYCLE, TimeUnit.SECONDS);
    }

    /**
     * 指标收集线程
     */
    private static class JstormMetricCollectExecutor implements Callable<Void> {

        private TaskPO taskPO;
        private JstormProcessPO jstormProcessPO;
        private CommonHbaseRecord taskMetricHbaseRecord;
        private Calendar currentCalendar;
        private long currentTimeStamp;
        private String currentMinute;
        private boolean isZkleader;

        public JstormMetricCollectExecutor(TaskPO taskPO,JstormProcessPO jstormProcessPO) {
            this.taskPO = taskPO;
            this.jstormProcessPO = jstormProcessPO;
            this.currentCalendar = DateUtil.getCalendar(DateUtil.getCurrentDateTime());
            this.currentTimeStamp = currentCalendar.getTime().getTime();
            this.currentMinute = new SimpleDateFormat("yyyyMMddHHmm").format(currentCalendar.getTime()) + "00";
            this.isZkleader = LeaderSelecter.getInstance().isLeader();
        }

        public Void call() throws Exception {
            NimbusClient nimbusClient = null;
            try{
                if(taskPO.getTaskStatus() != TaskStatusEnum.RUNNING.getValue()){
                    return null;
                }

                JstormTaskConfigDTO taskConfigDTO = JSONObject.parseObject(jstormProcessPO.getTaskConfig(), JstormTaskConfigDTO.class);
                if(taskConfigDTO == null){
                    return null;
                }

                String topId = jstormProcessPO.getTopId();
                taskMetricHbaseRecord = buildTaskMetricHbaseRecord(topId);

                TopologyInfo topologyInfo = null;
                nimbusClient = JStormClusterBusiness.getNimBusClientWithRetry(taskConfigDTO.getJstormZkHost(),taskConfigDTO.getJstormZkPort(),taskConfigDTO.getJstormZkRoot(),2,2);
                if(nimbusClient != null){
                    topologyInfo = JStormClusterBusiness.getTopologyInfoWithRetry(nimbusClient,topId);
                    if(topologyInfo == null){
                        LOGGER.error("JstormMetricCollectExecutor 获取topologyInfo 失败! zkRoot=" + taskConfigDTO.getJstormZkRoot());
                    }
                }else{
                    LOGGER.error("JstormMetricCollectExecutor 获取nimbus client 失败! zkRoot=" + taskConfigDTO.getJstormZkRoot());
                }

                collectTopologyMetic(topologyInfo, topId);
                collectComponentMetic(topologyInfo, topId);
                collectWorkerMetic(topologyInfo, topId);
                processThreadStatus(topologyInfo, topId);

                if (isZkleader) {
                    HBaseRecordUtils.send(taskMetricHbaseRecord);
                }
            }catch (Throwable e){
                LOGGER.error("JstormMetricCollectExecutor is error",e);
            }finally {
                if(nimbusClient!=null){
                    nimbusClient.close();
                }
            }
            return null;
        }

        /**
         * 建立一个 CommonHbaseRecord
         * @param topId
         * @return
         */
        private CommonHbaseRecord buildTaskMetricHbaseRecord(String topId){

            String reversalDateStr = HBaseUtils.getThisTimeDesc(currentTimeStamp) + "";
            String rowKey = MoniterContant.TASK_METRIC_JSTORM_ROWKEY + reversalDateStr +  MoniterContant.ROWKEY_SPLIT + topId + MoniterContant.ROWKEY_SPLIT;
            CommonHbaseRecord commonHbaseRecord = new CommonHbaseRecord(MoniterContant.TASK_METRIC_HTABLE_NAME,rowKey);

            commonHbaseRecord.addHbaseCell(MoniterContant.TIME_METRIC,HBaseUtils.getBytes(currentMinute));
            commonHbaseRecord.addHbaseCell(MoniterContant.TIMESTAMP_METRIC,HBaseUtils.getBytes(currentTimeStamp+ ""));
            return commonHbaseRecord;
        }

        /**
         * 建立一个 CommonHbaseRecord
         * @param topId
         * @return
         */
        private CommonHbaseRecord buildWorkerEorrorHbaseRecord(String topId){

            String reversalDateStr = HBaseUtils.getThisTimeDesc(currentTimeStamp) + "";
            String rowKey = MoniterContant.WORKER_ERROR_JSTORM_ROWKEY + reversalDateStr +  MoniterContant.ROWKEY_SPLIT + topId + MoniterContant.ROWKEY_SPLIT;
            CommonHbaseRecord commonHbaseRecord = new CommonHbaseRecord(MoniterContant.WORKER_ERROR_HTABLE_NAME,rowKey);

            commonHbaseRecord.addHbaseCell(MoniterContant.TIME_METRIC,HBaseUtils.getBytes(currentMinute));
            commonHbaseRecord.addHbaseCell(MoniterContant.TIMESTAMP_METRIC,HBaseUtils.getBytes(currentTimeStamp+ ""));
            return commonHbaseRecord;
        }

        /**
         * 收集拓扑相关指标
         * @param topologyInfo
         * @param topId
         * @param currentTime
         */
        private void collectTopologyMetic(TopologyInfo topologyInfo, String topId){

            String SEND_TPS = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String RECV_TPS = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String NETTY_CLI_SEND_SPEED = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String NETTY_SRV_RECV_SPEED = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String EMMITTED_NUM = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String FULL_GC = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String MEMORY_USED = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String HEAP_MEMORY_USED = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String CPU_USED_RATIO = MoniterContant.METRIC_COLLECT_FAILVALUE;

            if(topologyInfo != null){
                MetricInfo topologyMetrics = topologyInfo.get_metrics().get_topologyMetric();
                JstormTopologyMetric jstormTopologyMetric = JstormMetricBusiness.buildSummaryMetrics(topologyMetrics, MoniterContant.METRIC_COLLECT_CYCLE);

                SEND_TPS = jstormTopologyMetric.getMetrics().get(MetricDef.SEND_TPS);
                RECV_TPS = jstormTopologyMetric.getMetrics().get(MetricDef.RECV_TPS);
                NETTY_CLI_SEND_SPEED = jstormTopologyMetric.getMetrics().get(MetricDef.NETTY_CLI_SEND_SPEED);
                NETTY_SRV_RECV_SPEED = jstormTopologyMetric.getMetrics().get(MetricDef.NETTY_SRV_RECV_SPEED);
                EMMITTED_NUM = jstormTopologyMetric.getMetrics().get(MetricDef.EMMITTED_NUM);
                FULL_GC = jstormTopologyMetric.getMetrics().get(MetricDef.FULL_GC);
                MEMORY_USED = JstormMetricBusiness.getMemNumStr(jstormTopologyMetric.getMetrics().get(MetricDef.MEMORY_USED));
                HEAP_MEMORY_USED = JstormMetricBusiness.getMemNumStr(jstormTopologyMetric.getMetrics().get(MetricDef.HEAP_MEMORY_USED));
                CPU_USED_RATIO = jstormTopologyMetric.getMetrics().get(MetricDef.CPU_USED_RATIO);
            }

            MetricReportContainer.addDefaultGroupMetric( topId, MetricDef.SEND_TPS, currentTimeStamp, SEND_TPS);
            addTaskMetricHbaseCell(MetricDef.SEND_TPS, SEND_TPS);
            MetricReportContainer.addDefaultGroupMetric( topId, MetricDef.RECV_TPS, currentTimeStamp,RECV_TPS);
            addTaskMetricHbaseCell(MetricDef.RECV_TPS, RECV_TPS);
            MetricReportContainer.addDefaultGroupMetric( topId, MetricDef.NETTY_CLI_SEND_SPEED,currentTimeStamp, NETTY_CLI_SEND_SPEED);
            addTaskMetricHbaseCell(MetricDef.NETTY_CLI_SEND_SPEED, NETTY_CLI_SEND_SPEED);
            MetricReportContainer.addDefaultGroupMetric( topId, MetricDef.NETTY_SRV_RECV_SPEED,currentTimeStamp, NETTY_SRV_RECV_SPEED);
            addTaskMetricHbaseCell(MetricDef.NETTY_SRV_RECV_SPEED, NETTY_SRV_RECV_SPEED);
            MetricReportContainer.addDefaultGroupMetric( topId, MetricDef.EMMITTED_NUM,currentTimeStamp, EMMITTED_NUM);
            addTaskMetricHbaseCell(MetricDef.EMMITTED_NUM, EMMITTED_NUM);
            MetricReportContainer.addDefaultGroupMetric( topId, MetricDef.FULL_GC,currentTimeStamp, FULL_GC);
            addTaskMetricHbaseCell(MetricDef.FULL_GC, FULL_GC);
            MetricReportContainer.addDefaultGroupMetric( topId, MetricDef.MEMORY_USED, currentTimeStamp,MEMORY_USED);
            addTaskMetricHbaseCell(MetricDef.MEMORY_USED, MEMORY_USED);
            MetricReportContainer.addDefaultGroupMetric( topId, MetricDef.HEAP_MEMORY_USED, currentTimeStamp,HEAP_MEMORY_USED);
            addTaskMetricHbaseCell(MetricDef.HEAP_MEMORY_USED, HEAP_MEMORY_USED);
            MetricReportContainer.addDefaultGroupMetric( topId, MetricDef.CPU_USED_RATIO,currentTimeStamp, CPU_USED_RATIO);
            addTaskMetricHbaseCell(MetricDef.CPU_USED_RATIO, CPU_USED_RATIO);
        }

        /**
         * 收集组件相关指标
         * @param topologyInfo
         * @param topId
         * @param currentTime
         */
        private void collectComponentMetic(TopologyInfo topologyInfo, String topId) {

            String SEND_TPS = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String RECV_TPS = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String TUPLE_LIEF_CYCLE = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String PROCESS_LATENCY = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String EXECUTE_TIME = MoniterContant.METRIC_COLLECT_FAILVALUE;

            Map<String, String> COMP_SEND_TPS = Maps.newHashMap();
            Map<String, String> COMP_RECV_TPS = Maps.newHashMap();
            Map<String, String> COMP_TUPLE_LIEF_CYCLE = Maps.newHashMap();
            Map<String, String> COMP_PROCESS_LATENCY = Maps.newHashMap();
            Map<String, String> COMP_EXECUTE_TIME = Maps.newHashMap();

            if(topologyInfo != null){

                MetricInfo componentMetrics = topologyInfo.get_metrics().get_componentMetric();
                List<JstormComponentMetric> jstormComponentMetrics = JstormMetricBusiness.buildComponentMetrics(componentMetrics,  MoniterContant.METRIC_COLLECT_CYCLE, topologyInfo.get_components());
                for (JstormComponentMetric component : jstormComponentMetrics) {
                    Map<String, String> metricMap = component.getMetrics();
                    String componentName = component.getComponentName();
                    if (metricMap == null || metricMap.size() == 0 || StringUtils.isBlank(componentName) || componentName.equals("__topology_master") || componentName.equals("__acker")){
                        continue;
                    }

                    SEND_TPS = metricMap.get( MetricDef.SEND_TPS);
                    RECV_TPS = metricMap.get( MetricDef.RECV_TPS);
                    TUPLE_LIEF_CYCLE = metricMap.get( MetricDef.TUPLE_LIEF_CYCLE);
                    PROCESS_LATENCY = metricMap.get( MetricDef.PROCESS_LATENCY);
                    EXECUTE_TIME =metricMap.get( MetricDef.EXECUTE_TIME);

                    COMP_SEND_TPS.put(componentName,SEND_TPS);
                    COMP_RECV_TPS.put(componentName,RECV_TPS);
                    COMP_TUPLE_LIEF_CYCLE .put(componentName,TUPLE_LIEF_CYCLE);
                    COMP_PROCESS_LATENCY .put(componentName,PROCESS_LATENCY);
                    COMP_EXECUTE_TIME.put(componentName,EXECUTE_TIME);
                }
            }

            addTaskMetricHbaseCell("comp_" + MetricDef.SEND_TPS,COMP_SEND_TPS);
            addTaskMetricHbaseCell("comp_" + MetricDef.RECV_TPS,COMP_RECV_TPS);
            addTaskMetricHbaseCell("comp_" + MetricDef.TUPLE_LIEF_CYCLE,COMP_TUPLE_LIEF_CYCLE);
            addTaskMetricHbaseCell("comp_" + MetricDef.PROCESS_LATENCY,COMP_PROCESS_LATENCY);
            addTaskMetricHbaseCell("comp_" + MetricDef.EXECUTE_TIME,COMP_EXECUTE_TIME);
        }

        /**
         * 收集workder相关指标
         * @param topologyInfo
         * @param topId
         * @param currentTime
         */
        private void collectWorkerMetic(TopologyInfo topologyInfo, String topId) {

            String FULL_GC = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String MEMORY_USED = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String HEAP_MEMORY_USED = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String GCCOUNT = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String GCTIME = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String NETTY_CLI_SEND_SPEED = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String NETTY_SRV_RECV_SPEED = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String RECV_CTRL_QUEUE = MoniterContant.METRIC_COLLECT_FAILVALUE;
            String SEND_QUEUE = MoniterContant.METRIC_COLLECT_FAILVALUE;

            Map<String, String> WORKER_FULL_GC = Maps.newHashMap();
            Map<String, String> WORKER_MEMORY_USED = Maps.newHashMap();
            Map<String, String> WORKER_HEAP_MEMORY_USED = Maps.newHashMap();
            Map<String, String> WORKER_GCCOUNT = Maps.newHashMap();
            Map<String, String> WORKER_GCTIME = Maps.newHashMap();
            Map<String, String> WORKER_NETTY_CLI_SEND_SPEED = Maps.newHashMap();
            Map<String, String> WORKER_NETTY_SRV_RECV_SPEED = Maps.newHashMap();
            Map<String, String> WORKER_RECV_CTRL_QUEUE = Maps.newHashMap();
            Map<String, String> WORKER_SEND_QUEUE = Maps.newHashMap();

            if(topologyInfo != null){
                MetricInfo workerMetrics = topologyInfo.get_metrics().get_workerMetric();
                List<JstormWorkerMetric> jstormWorkerMetrics = JstormMetricBusiness.buildWorkerMetrics(workerMetrics, topId,  MoniterContant.METRIC_COLLECT_CYCLE);

                for (JstormWorkerMetric jstormWorkerMetric : jstormWorkerMetrics) {
                    Map<String, String> metricMap = jstormWorkerMetric.getMetrics();
                    String host = jstormWorkerMetric.getHost();
                    String port = jstormWorkerMetric.getPort();
                    if (metricMap == null || metricMap.size() == 0 || StringUtils.isBlank(host) || StringUtils.isBlank(port)) {
                        continue;
                    }

                    String worker = host + ":" + port;

                    FULL_GC = metricMap.get( MetricDef.FULL_GC);
                    MEMORY_USED = JstormMetricBusiness.getMemNumStr(metricMap.get( MetricDef.MEMORY_USED));
                    HEAP_MEMORY_USED = JstormMetricBusiness.getMemNumStr(metricMap.get( MetricDef.HEAP_MEMORY_USED));
                    GCCOUNT = metricMap.get(METRIC_WORKER_GC_COUNT);
                    GCTIME = metricMap.get(METRIC_WORKER_GC_TIME);
                    NETTY_CLI_SEND_SPEED = metricMap.get( MetricDef.NETTY_CLI_SEND_SPEED);
                    NETTY_SRV_RECV_SPEED = metricMap.get( MetricDef.NETTY_SRV_RECV_SPEED);
                    RECV_CTRL_QUEUE = metricMap.get( MetricDef.SEND_TPS);
                    SEND_QUEUE = metricMap.get( MetricDef.SEND_TPS);

                    WORKER_FULL_GC.put(worker,FULL_GC);
                    WORKER_MEMORY_USED.put(worker,MEMORY_USED);
                    WORKER_HEAP_MEMORY_USED .put(worker,HEAP_MEMORY_USED);
                    WORKER_GCCOUNT.put(worker,GCCOUNT);
                    WORKER_GCTIME.put(worker,GCTIME);
                    WORKER_NETTY_CLI_SEND_SPEED.put(worker,NETTY_CLI_SEND_SPEED);
                    WORKER_NETTY_SRV_RECV_SPEED .put(worker,NETTY_SRV_RECV_SPEED);
                    WORKER_RECV_CTRL_QUEUE.put(worker,RECV_CTRL_QUEUE);
                    WORKER_SEND_QUEUE.put(worker,SEND_QUEUE);
                }
            }

            addTaskMetricHbaseCell("worker_" + MetricDef.FULL_GC,WORKER_FULL_GC);
            addTaskMetricHbaseCell("worker_" + MetricDef.MEMORY_USED,WORKER_MEMORY_USED);
            addTaskMetricHbaseCell("worker_" + MetricDef.HEAP_MEMORY_USED,WORKER_HEAP_MEMORY_USED);
            addTaskMetricHbaseCell("worker_" + METRIC_WORKER_GC_COUNT,WORKER_GCCOUNT);
            addTaskMetricHbaseCell("worker_" + METRIC_WORKER_GC_TIME,WORKER_GCTIME);
            addTaskMetricHbaseCell("worker_" + MetricDef.NETTY_CLI_SEND_SPEED,WORKER_NETTY_CLI_SEND_SPEED);
            addTaskMetricHbaseCell("worker_" + MetricDef.NETTY_SRV_RECV_SPEED,WORKER_NETTY_SRV_RECV_SPEED);
            addTaskMetricHbaseCell("worker_" + MetricDef.RECV_CTRL_QUEUE,WORKER_RECV_CTRL_QUEUE);
            addTaskMetricHbaseCell("worker_" + MetricDef.SEND_QUEUE,WORKER_SEND_QUEUE);
        }

        private void addTaskMetricHbaseCell(String metricName, String metriValue){
            taskMetricHbaseRecord.addHbaseCell(metricName,HBaseUtils.getBytes(metriValue));
        }

        private void addTaskMetricHbaseCell(String metricName, Map<String,String> metriValue){
            taskMetricHbaseRecord.addHbaseCell(metricName,HBaseUtils.getBytes(JSONObject.toJSONString(metriValue)));
        }

        /**
         * 处理线程状态变更。worker迁移记录
         * @param topologyInfo
         * @param topId
         */
        private void processThreadStatus(TopologyInfo topologyInfo, String topId) {

            //如果前边一个状态拓扑完蛋了，则获得不了前一个状态。所有用-1表示currentThreadStatus放入指标。
            //只有能获得前一个状态时，才有比较两个worker的必要

            if(topologyInfo ==null) {
                workerErrorHistoryCache.remove(topId);
                MetricReportContainer.addDefaultGroupMetric(topId, METRIC_DEF_THREAD_STATUS, currentTimeStamp, "-1");
                return;
            }

            Map<Integer,JstormCompenentTaskDTO> currentThreadStatuss = JStormTopologyBusiness.buildTaskInfo(topologyInfo);
            Map<Integer,JstormCompenentTaskDTO> beforeThreadStatuss = null;

            LineReportDTO lineReportDTO = MetricReportContainer.getReport(topId, METRIC_DEF_THREAD_STATUS);

            for(LineReportTimelineDTO workerTimeline: lineReportDTO.getGroupToTimeline().values()){
                MetricValueDTO lastMetric = workerTimeline.getLastMetric();
                if(lastMetric != null && !lastMetric.getMetricValue().equals("-1")){
                     beforeThreadStatuss = JSONObject.parseObject(lastMetric.getMetricValue(), new TypeReference<Map<Integer,JstormCompenentTaskDTO>>(){});
                }
            }

            if(beforeThreadStatuss!=null){
                boolean hasError = false;

                HashMap<String,String>  workerToErrorInfo = Maps.newHashMap();
                HashMap<String,Integer> workerToErrorType = Maps.newHashMap();

                if(workerErrorHistoryCache.putIfAbsent(topId,workerToErrorType) != null){
                    workerToErrorType = workerErrorHistoryCache.get(topId);
                }

                for(Map.Entry<Integer,JstormCompenentTaskDTO> currentThreadStatus:currentThreadStatuss.entrySet()){

                    //获得前一分钟时候这个task的状态值
                    Integer threadId = currentThreadStatus.getKey();

                    JstormCompenentTaskDTO currentStatus = currentThreadStatus.getValue();
                    JstormCompenentTaskDTO beforeStatus = beforeThreadStatuss.get(threadId);

                    hasError = workerIsHasError(workerToErrorInfo, workerToErrorType, currentStatus, beforeStatus);
                }

                if(hasError){
                    if(!workerToErrorInfo.isEmpty()){
                        for(String info:workerToErrorInfo.values()){
                            if (LeaderSelecter.getInstance().isLeader()) {

                                LOGGER.error("JstormMetricCollectImpl processThreadStatus alarm " + info);

                                CommonHbaseRecord workerEorrorHbaseRecord = buildWorkerEorrorHbaseRecord(topId);
                                workerEorrorHbaseRecord.addHbaseCell(MoniterContant.WORKER_ERROR_METRIC,HBaseUtils.getBytes(info));
                                HBaseRecordUtils.send(workerEorrorHbaseRecord);
                            }
                        }
                    }
                }else{
                    //如果曾经有过问题
                    if(workerErrorHistoryCache.containsKey(topId) && !workerErrorHistoryCache.get(topId).isEmpty()){

                        workerErrorHistoryCache.remove(topId);
                        if (LeaderSelecter.getInstance().isLeader()) {
                            String info = "所有worker已恢复执行！";

                            LOGGER.error("JstormMetricCollectImpl processThreadStatus recovery" + info);

                            CommonHbaseRecord workerEorrorHbaseRecord = buildWorkerEorrorHbaseRecord(topId);
                            workerEorrorHbaseRecord.addHbaseCell(MoniterContant.WORKER_ERROR_METRIC,HBaseUtils.getBytes(info));
                            HBaseRecordUtils.send(workerEorrorHbaseRecord);
                        }
                    }
                }
            }

            MetricReportContainer.addDefaultGroupMetric(topId, METRIC_DEF_THREAD_STATUS, currentTimeStamp,  JSONObject.toJSONString(currentThreadStatuss));
        }

        /**
         * 检查 worker是否有错误，并返回结果
         * @param workerToErrorInfo
         * @param workerToErrorType
         * @param currentStatus
         * @param beforeStatus
         * @return
         */
        private boolean workerIsHasError(HashMap<String,String>  workerToErrorInfo, HashMap<String, Integer> workerToErrorType, JstormCompenentTaskDTO currentStatus, JstormCompenentTaskDTO beforeStatus) {
            String currentWorker =  currentStatus.getHost() + ":" + currentStatus.getPort();
            String beforeWorker =  beforeStatus.getHost() + ":" + beforeStatus.getPort();

            boolean hasError = false;
            if (!beforeStatus.getStatus().toLowerCase().equals(EngineContant.WORKER_STATUS_INACTIVE) && currentStatus.getStatus().toLowerCase().equals(EngineContant.WORKER_STATUS_INACTIVE)){
                // 发生异常
                hasError = true;
                // 如果放入过了，就不在放。避免一直疯狂提醒。
                if(!workerToErrorType.containsKey(currentWorker)
                        ||  (workerToErrorType.containsKey(currentWorker) && !workerToErrorType.get(currentWorker).equals(EngineContant.WORKER_STATUS_INACTIVE))){
                    workerToErrorType.put(currentWorker, JstormWorkerErrorEnum.INACTIVE.getValue());
                    workerToErrorInfo.put(currentWorker, buildWorkerErrorInfo( currentWorker, beforeWorker, JstormWorkerErrorEnum.INACTIVE.getValue()));
                }
            }else if(beforeStatus.getStatus().toLowerCase().equals(EngineContant.WORKER_STATUS_STARTING) && currentStatus.getStatus().toLowerCase().equals(EngineContant.WORKER_STATUS_STARTING)){
                // 长时间starting
                hasError = true;
                // 如果放入过了，就不在放。避免一直疯狂提醒。
                if(!workerToErrorType.containsKey(currentWorker)
                        ||  (workerToErrorType.containsKey(currentWorker) && !workerToErrorType.get(currentWorker).equals(EngineContant.WORKER_STATUS_STARTING))){
                    workerToErrorType.put(currentWorker, JstormWorkerErrorEnum.STARTING.getValue());
                    workerToErrorInfo.put(currentWorker, buildWorkerErrorInfo( currentWorker, beforeWorker, JstormWorkerErrorEnum.INACTIVE.getValue()));
                }
            }else if ( !(currentWorker) .equals(beforeWorker)){
                // 进行了迁移
                hasError = true;
                workerToErrorInfo.put(currentWorker, buildWorkerErrorInfo( currentWorker, beforeWorker, JstormWorkerErrorEnum.TRANSFER.getValue()));
            }else if (currentStatus.getUptimeSeconds() < beforeStatus.getUptimeSeconds()) {
                // 已在同一机器上重启
                hasError = true;
                workerToErrorInfo.put(currentWorker, buildWorkerErrorInfo( currentWorker, beforeWorker, JstormWorkerErrorEnum.RESTART.getValue()));
            }
            return hasError;
        }

        /**
         * 根据错误类型构造错误信息
         * @param currentWorker
         * @param beforeWorker
         * @param errorType
         * @return
         */
        private String buildWorkerErrorInfo(String currentWorker,String beforeWorker,Integer errorType){
            if(errorType == JstormWorkerErrorEnum.TRANSFER.getValue()){
                return beforeWorker+ " 成功迁移到 "+ currentWorker;
            } else if(errorType == JstormWorkerErrorEnum.INACTIVE.getValue()){
                return beforeWorker+ " inactive，请及时处理！";
            } else if(errorType == JstormWorkerErrorEnum.RESTART.getValue()){
                return beforeWorker+ " 自动重启成功！" ;
            } else if(errorType == JstormWorkerErrorEnum.STARTING.getValue()){
                return beforeWorker+ " 长时间 starting！";
            }else{
                return "所有worker已恢复执行！";
            }
        }
    }
}
