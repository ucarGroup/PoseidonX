package com.ucar.streamsuite.moniter.service.impl;

import com.alibaba.jstorm.metric.MetricDef;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ucar.streamsuite.common.hbase.util.HBaseRecordUtils;
import com.ucar.streamsuite.common.hbase.util.HBaseUtils;
import com.ucar.streamsuite.common.hbase.vo.Column;
import com.ucar.streamsuite.common.hbase.vo.RowVo;
import com.ucar.streamsuite.common.hbase.vo.ScanVo;
import com.ucar.streamsuite.common.util.DateUtil;
import com.ucar.streamsuite.moniter.constants.MoniterContant;
import com.ucar.streamsuite.moniter.dto.LineReportDTO;
import com.ucar.streamsuite.moniter.dto.LineReportTimelineDTO;
import com.ucar.streamsuite.moniter.dto.MetricValueDTO;
import com.ucar.streamsuite.moniter.service.JstormReportService;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Description: jstorm 引擎报表服务类
 * Created on 2018/1/30 上午10:59
 *
 *
 */
@Service
public class JstormReportServiceImpl implements JstormReportService {

    private static final Logger LOGGER = LoggerFactory.getLogger(JstormReportServiceImpl.class);

    private static ThreadLocal<Integer> searchTimeBetweenLength = new ThreadLocal<Integer>();

    @Override
    public List<LineReportDTO> getReportDataByTime(String topId, Date beginTime,Date endTime) {
        List<LineReportDTO> lineReportDTOs = Lists.newArrayList();
        if (StringUtils.isBlank(topId) || beginTime == null || endTime == null) {
            return lineReportDTOs;
        }

        Long timeBetween = (endTime.getTime() - beginTime.getTime())/ (1000 * 60);
        if(timeBetween >= MoniterContant.JSTORM_REPORT_MAX_QUERY_TIME){
            return lineReportDTOs;
        }

        //存放时间间隔
        searchTimeBetweenLength.set(timeBetween.intValue());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        String beginTimeStr = sdf.format(beginTime) + "00";
        String endTimeStr =sdf.format(endTime) + "00";

        String startRow = MoniterContant.TASK_METRIC_JSTORM_ROWKEY + HBaseUtils.getReversalTimeStr(endTimeStr) + MoniterContant.ROWKEY_SPLIT;
        String endRow = MoniterContant.TASK_METRIC_JSTORM_ROWKEY + HBaseUtils.getReversalTimeStr(beginTimeStr) + MoniterContant.ROWKEY_SPLIT + "~";
        Integer limit = MoniterContant.JSTORM_REPORT_MAXROW_SIZE;
        String tableName = MoniterContant.TASK_METRIC_HTABLE_NAME;

        ScanVo scanVo =  HBaseRecordUtils.getScanVO(tableName ,endTimeStr,startRow,endRow,limit);
        List<RowVo> scanResults = HBaseRecordUtils.getScanResult(scanVo);
        return filterResultAndBuildReports(topId, beginTimeStr, endTimeStr, scanResults);
    }

    @Override
    public List<String> getWorkerErrorData(String topId, Date beginTime, Date endTime){
        List<String> workerErrorDatas = Lists.newArrayList();
        if (StringUtils.isBlank(topId) || beginTime == null || endTime == null) {
            return workerErrorDatas;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        String beginTimeStr = sdf.format(beginTime) + "00";
        String endTimeStr =sdf.format(endTime) + "00";

        String startRow = MoniterContant.WORKER_ERROR_JSTORM_ROWKEY + HBaseUtils.getReversalTimeStr(endTimeStr) + MoniterContant.ROWKEY_SPLIT;
        String endRow = MoniterContant.WORKER_ERROR_JSTORM_ROWKEY + HBaseUtils.getReversalTimeStr(beginTimeStr) + MoniterContant.ROWKEY_SPLIT + "~";
        Integer limit = MoniterContant.JSTORM_REPORT_MAXROW_SIZE;
        String tableName = MoniterContant.WORKER_ERROR_HTABLE_NAME;

        ScanVo scanVo =  HBaseRecordUtils.getScanVO(tableName ,endTimeStr,startRow,endRow,limit);
        List<RowVo> scanResults = HBaseRecordUtils.getScanResult(scanVo);

        TreeMap<String,String> timeToErrorInfo = new TreeMap<String,String>();
        for (RowVo rowVo : scanResults) {
            String rowKey = rowVo.getRowKey();
            String[] rowArr = rowKey.split("#");
            String rowTopId = rowArr[rowArr.length-1];
            if (!rowTopId.equals(topId)) {
                continue;
            }
            Map<String,String> columnDateToMap = rowVo.convertColumnDateToMap();
            if (columnDateToMap.isEmpty()) {
                continue;
            }
            String TIME = columnDateToMap.get(MoniterContant.TIME_METRIC);
            String WORKER_ERROR_METRIC = columnDateToMap.get(MoniterContant.WORKER_ERROR_METRIC);
            timeToErrorInfo.put(TIME,WORKER_ERROR_METRIC);
        }

        if(!timeToErrorInfo.isEmpty()){
            for(Map.Entry<String,String> errorInfo:timeToErrorInfo.entrySet()){
                workerErrorDatas.add( DateUtil.yyyymmddhhmm(errorInfo.getKey()) + " [" + errorInfo.getValue() +"]");
            }
        }
        return workerErrorDatas;
    }

    /**
     * 过滤并返回报表结果
     * @param beginTime
     * @param endTime
     * @return
     */
    private List<LineReportDTO> filterResultAndBuildReports(String topId, String beginTime,String endTime, List<RowVo> scanResults) {
        List<LineReportDTO> rsLineReportDTOs = Lists.newArrayList();

        String[] reportTitles = new String[]{MetricDef.SEND_TPS,MetricDef.RECV_TPS,MetricDef.NETTY_CLI_SEND_SPEED,
                MetricDef.NETTY_SRV_RECV_SPEED,MetricDef.FULL_GC,MetricDef.MEMORY_USED,MetricDef.HEAP_MEMORY_USED,
                MetricDef.CPU_USED_RATIO};
        Map<String,LineReportDTO> reportDTOs = initReports(reportTitles);

        try {
            // 先整理从hbase拿出来的数据
            Map<String,Map<String,String>> timeToMetrics = Maps.newHashMap();
            for (RowVo rowVo : scanResults) {
                String rowKey = rowVo.getRowKey();
                String[] rowArr = rowKey.split("#");
                String rowTopId = rowArr[rowArr.length-1];
                if (!rowTopId.equals(topId)) {
                    continue;
                }
                Map<String,String> columnDateToMap = rowVo.convertColumnDateToMap();
                if (columnDateToMap.isEmpty()) {
                    continue;
                }
                String TIME = columnDateToMap.get(MoniterContant.TIME_METRIC);
                timeToMetrics.put(TIME,columnDateToMap);
            }

            // 依次加入每分钟的数据
            for(int i=0;i<searchTimeBetweenLength.get();i++){
                String time = DateUtil.addMinTime(beginTime,i);

                // 如果是 -1 前端需要判断放null值，未获取到此分钟统计指标的值
                String SEND_TPS = MoniterContant.REPORT_DEFAULT_METRICVALUE;
                String RECV_TPS = MoniterContant.REPORT_DEFAULT_METRICVALUE;
                String NETTY_CLI_SEND_SPEED = MoniterContant.REPORT_DEFAULT_METRICVALUE;
                String NETTY_SRV_RECV_SPEED = MoniterContant.REPORT_DEFAULT_METRICVALUE;
                String FULL_GC = MoniterContant.REPORT_DEFAULT_METRICVALUE;
                String MEMORY_USED = MoniterContant.REPORT_DEFAULT_METRICVALUE;
                String HEAP_MEMORY_USED = MoniterContant.REPORT_DEFAULT_METRICVALUE;
                String CPU_USED_RATIO = MoniterContant.REPORT_DEFAULT_METRICVALUE;

                Map<String,String> columnDateToMap = timeToMetrics.get(time);
                if(columnDateToMap != null){
                    SEND_TPS = columnDateToMap.get(MetricDef.SEND_TPS);
                    RECV_TPS = columnDateToMap.get(MetricDef.RECV_TPS);
                    NETTY_CLI_SEND_SPEED = columnDateToMap.get(MetricDef.NETTY_CLI_SEND_SPEED);
                    NETTY_SRV_RECV_SPEED = columnDateToMap.get(MetricDef.NETTY_SRV_RECV_SPEED);
                    FULL_GC = columnDateToMap.get(MetricDef.FULL_GC);
                    MEMORY_USED = columnDateToMap.get(MetricDef.MEMORY_USED);
                    HEAP_MEMORY_USED = columnDateToMap.get(MetricDef.HEAP_MEMORY_USED);
                    CPU_USED_RATIO = columnDateToMap.get(MetricDef.CPU_USED_RATIO);
                }

                // 加入 top的报表 (转换成时间戳 因为页面需要用时间戳展示报告)
                long timeStamp = DateUtil.getCalendar(time).getTime().getTime();
                addDefaultGroupMetric(reportDTOs.get(MetricDef.SEND_TPS), MetricDef.SEND_TPS,  timeStamp,  SEND_TPS);
                addDefaultGroupMetric(reportDTOs.get(MetricDef.RECV_TPS),  MetricDef.RECV_TPS,  timeStamp,  RECV_TPS);
                addDefaultGroupMetric(reportDTOs.get(MetricDef.NETTY_CLI_SEND_SPEED), MetricDef.NETTY_CLI_SEND_SPEED,  timeStamp,  NETTY_CLI_SEND_SPEED);
                addDefaultGroupMetric(reportDTOs.get(MetricDef.NETTY_SRV_RECV_SPEED), MetricDef.NETTY_SRV_RECV_SPEED,  timeStamp,  NETTY_SRV_RECV_SPEED);
                addDefaultGroupMetric(reportDTOs.get(MetricDef.FULL_GC),  MetricDef.FULL_GC,  timeStamp,  FULL_GC);
                addDefaultGroupMetric(reportDTOs.get(MetricDef.MEMORY_USED), MetricDef.MEMORY_USED,  timeStamp,  MEMORY_USED);
                addDefaultGroupMetric(reportDTOs.get(MetricDef.HEAP_MEMORY_USED), MetricDef.HEAP_MEMORY_USED,  timeStamp,  HEAP_MEMORY_USED);
                addDefaultGroupMetric(reportDTOs.get(MetricDef.CPU_USED_RATIO), MetricDef.CPU_USED_RATIO,  timeStamp,  CPU_USED_RATIO);
            }

        } catch (Exception e) {
            LOGGER.error("JstormReportServiceImpl getNeedData", e);
        }
        return Lists.newArrayList(reportDTOs.values());
    }

    /**
     * 初始化报表数据
     * @param rowVo
     */
    private Map<String,LineReportDTO> initReports(String[] reportTitles){
        Map<String,LineReportDTO> lineReportDTOs = Maps.newHashMap();
        for(String reportTitle:reportTitles){
            LineReportDTO sendTpsReportDTO = new LineReportDTO(reportTitle);
            lineReportDTOs.put(reportTitle,sendTpsReportDTO);
        }
        return lineReportDTOs;
    }

    /**
     * 将指标的值放入报表
     */
    private void addMetric(LineReportDTO lineReportDTO,long metricTimeStamp,Map<String, String> currentTimeDatas) {
        if(currentTimeDatas == null){
            return;
        }
        for(Map.Entry<String,String> currentTimeData:currentTimeDatas.entrySet()){
            String group = currentTimeData.getKey();
            String metricValue = currentTimeData.getValue();
            LineReportTimelineDTO timeline = lineReportDTO.getTimeLineByGroup(group);
            if(timeline == null){
                timeline = new LineReportTimelineDTO(currentTimeData.getKey(), searchTimeBetweenLength.get());
                lineReportDTO.addTimeLine(timeline);
            }
            timeline.append(new MetricValueDTO(metricTimeStamp,metricValue));
        }
    }

    /**
     * 添加一个默认分组的metric
     * @param topId
     * @param currentTime
     * @param metricName
     * @param metriValue
     */
    private void addDefaultGroupMetric(LineReportDTO lineReportDTO, String metricName, long metricTimeStamp, String metriValue){
        Map<String,String> metricDatas = Maps.newHashMap();
        metricDatas.put(MoniterContant.REPORT_DEFAULT_GROUPNAME, metriValue);
        addMetric(lineReportDTO, metricTimeStamp, metricDatas);
    }
}
