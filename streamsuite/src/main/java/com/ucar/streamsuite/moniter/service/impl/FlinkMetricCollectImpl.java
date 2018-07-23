package com.ucar.streamsuite.moniter.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.ucar.streamsuite.common.constant.TaskStatusEnum;
import com.ucar.streamsuite.common.hbase.CommonHbaseRecord;
import com.ucar.streamsuite.common.hbase.util.HBaseRecordUtils;
import com.ucar.streamsuite.common.hbase.util.HBaseUtils;
import com.ucar.streamsuite.common.util.DateUtil;
import com.ucar.streamsuite.common.util.LeaderSelecter;
import com.ucar.streamsuite.dao.mysql.FlinkProcessDao;
import com.ucar.streamsuite.dao.mysql.TaskDao;
import com.ucar.streamsuite.engine.business.FlinkWebClientBusiness;
import com.ucar.streamsuite.engine.dto.FlinkJobDTO;
import com.ucar.streamsuite.engine.dto.FlinkTaskConfigDTO;
import com.ucar.streamsuite.engine.po.FlinkProcessPO;
import com.ucar.streamsuite.engine.service.impl.FlinkEngineServiceImpl;
import com.ucar.streamsuite.moniter.constants.MoniterContant;
import com.ucar.streamsuite.moniter.dto.FlinkVerticeMetricDto;
import com.ucar.streamsuite.task.po.TaskPO;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
public class FlinkMetricCollectImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkMetricCollectImpl.class);

    private static final ScheduledExecutorService metricCollector = Executors.newScheduledThreadPool(1);

    //使用的线程池个数较大 要保证规定时间内执行完
    private static final ExecutorService metricCollectExecutorService = Executors.newFixedThreadPool(20);

    private static FlinkMetricCollectImpl metricCollectImpl;

    @Autowired
    private TaskDao taskDao;
    @Autowired
    private FlinkProcessDao flinkProcessDao;

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
                    if (!LeaderSelecter.getInstance().isLeader()) {
                        return;
                    }
                    long begin = System.currentTimeMillis();

                    Set<Future> futures = Sets.newHashSet();
                    Map<TaskPO,FlinkProcessPO> taskToProcessMap = FlinkEngineServiceImpl.listAllTaskToProcess();
                    for(Map.Entry<TaskPO,FlinkProcessPO> taskToProcess:taskToProcessMap.entrySet()){
                        Future future = metricCollectExecutorService.submit(new FlinkMetricCollectExecutor(taskToProcess.getKey(),taskToProcess.getValue()));
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
                    //LOGGER.error("FlinkMetricCollectImpl startMetricCollector end" + " exeTime = " + (System.currentTimeMillis() - begin) /1000);
                } catch (Throwable e) {
                    LOGGER.error("FlinkMetricCollectImpl startMetricCollector is error", e);
                }
            }
        }, 1, MoniterContant.METRIC_COLLECT_CYCLE, TimeUnit.SECONDS);
    }

    /**
     * 指标收集线程
     */
    private static class FlinkMetricCollectExecutor implements Callable<Void> {

        private TaskPO taskPO;
        private FlinkProcessPO flinkProcessPO;
        private CommonHbaseRecord taskMetricHbaseRecord;
        private Calendar currentCalendar;
        private long currentTimeStamp;
        private String currentMinute;

        public FlinkMetricCollectExecutor(TaskPO taskPO,FlinkProcessPO flinkProcessPO) {
            this.taskPO = taskPO;
            this.flinkProcessPO = flinkProcessPO;
            this.currentCalendar = DateUtil.getCalendar(DateUtil.getCurrentDateTime());
            this.currentTimeStamp = currentCalendar.getTime().getTime();
            this.currentMinute = new SimpleDateFormat("yyyyMMddHHmm").format(currentCalendar.getTime()) + "00";
        }

        public Void call() throws Exception {
            try{
                if(taskPO.getTaskStatus() != TaskStatusEnum.RUNNING.getValue()){
                    return null;
                }

                FlinkTaskConfigDTO taskConfigDTO = JSONObject.parseObject(flinkProcessPO.getTaskConfig(), FlinkTaskConfigDTO.class);
                if(taskConfigDTO == null){
                    return null;
                }

                String jobId = flinkProcessPO.getJobId();
                taskMetricHbaseRecord = buildTaskMetricHbaseRecord(jobId);

                FlinkJobDTO flinkJobDTO = FlinkWebClientBusiness.getFlinkJobDtoWithRetry(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId());
                if(flinkJobDTO == null){
                    return null;
                }

                List<FlinkVerticeMetricDto> flinkVerticeMetricDtos = FlinkWebClientBusiness.getVerticeMetricsDtos(flinkProcessPO.getYarnAppId(),flinkProcessPO.getJobId());
                if(CollectionUtils.isEmpty(flinkVerticeMetricDtos)){
                    return null;
                }

                taskMetricHbaseRecord.addHbaseCell(MoniterContant.VERTICE_TO_METRIC,HBaseUtils.getBytes(JSONObject.toJSONString(flinkVerticeMetricDtos)));
                HBaseRecordUtils.send(taskMetricHbaseRecord);
            }catch (Throwable e){
                LOGGER.error("FlinkMetricCollectExecutor is error",e);
            }
            return null;
        }

        /**
         * 建立一个 CommonHbaseRecord
         * @param jobId
         * @return
         */
        private CommonHbaseRecord buildTaskMetricHbaseRecord(String jobId){
            String reversalDateStr = HBaseUtils.getThisTimeDesc(currentTimeStamp) + "";
            String rowKey = MoniterContant.TASK_METRIC_FLINK_ROWKEY + reversalDateStr +  MoniterContant.ROWKEY_SPLIT + jobId + MoniterContant.ROWKEY_SPLIT;
            CommonHbaseRecord commonHbaseRecord = new CommonHbaseRecord(MoniterContant.TASK_METRIC_HTABLE_NAME,rowKey);

            commonHbaseRecord.addHbaseCell(MoniterContant.TIME_METRIC,HBaseUtils.getBytes(currentMinute));
            commonHbaseRecord.addHbaseCell(MoniterContant.TIMESTAMP_METRIC,HBaseUtils.getBytes(currentTimeStamp+ ""));
            return commonHbaseRecord;
        }
    }
}
