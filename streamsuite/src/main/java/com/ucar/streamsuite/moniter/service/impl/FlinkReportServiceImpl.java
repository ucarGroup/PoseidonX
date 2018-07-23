package com.ucar.streamsuite.moniter.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ucar.streamsuite.common.hbase.util.HBaseRecordUtils;
import com.ucar.streamsuite.common.hbase.util.HBaseUtils;
import com.ucar.streamsuite.common.hbase.vo.RowVo;
import com.ucar.streamsuite.common.hbase.vo.ScanVo;
import com.ucar.streamsuite.common.util.DateUtil;
import com.ucar.streamsuite.moniter.dto.FlinkMetricDto;
import com.ucar.streamsuite.moniter.constants.MoniterContant;
import com.ucar.streamsuite.moniter.dto.*;
import com.ucar.streamsuite.moniter.service.FlinkReportService;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Description: flink 引擎报表服务类
 * Created on 2018/1/30 上午10:59
 *
 *
 */
@Service
public class FlinkReportServiceImpl implements FlinkReportService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkReportServiceImpl.class);

    private static ThreadLocal<Integer> searchTimeBetweenLength = new ThreadLocal<Integer>();

    @Override
    public List<MutilLineReportDTO> getReportDataByTime(String jobId,Date beginTime,Date endTime) {
        List<MutilLineReportDTO> lineReportDTOs = Lists.newArrayList();
        if (StringUtils.isBlank(jobId) || beginTime == null || endTime == null) {
            return lineReportDTOs;
        }

        Long timeBetween = (endTime.getTime() - beginTime.getTime())/ (1000 * 60);
        if(timeBetween >= MoniterContant.FLINK_REPORT_MAX_QUERY_TIME){
            return lineReportDTOs;
        }

        //存放时间间隔
        searchTimeBetweenLength.set(timeBetween.intValue());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        String beginTimeStr = sdf.format(beginTime) + "00";
        String endTimeStr =sdf.format(endTime) + "00";

        String startRow = MoniterContant.TASK_METRIC_FLINK_ROWKEY + HBaseUtils.getReversalTimeStr(endTimeStr) + MoniterContant.ROWKEY_SPLIT;
        String endRow = MoniterContant.TASK_METRIC_FLINK_ROWKEY + HBaseUtils.getReversalTimeStr(beginTimeStr) + MoniterContant.ROWKEY_SPLIT + "~";
        Integer limit = MoniterContant.FLINK_REPORT_MAXROW_SIZE;
        String tableName = MoniterContant.TASK_METRIC_HTABLE_NAME;

        ScanVo scanVo =  HBaseRecordUtils.getScanVO(tableName ,endTimeStr,startRow,endRow,limit);
        List<RowVo> scanResults = HBaseRecordUtils.getScanResult(scanVo);
        return filterResultAndBuildReports(jobId, beginTimeStr, endTimeStr, scanResults);
    }

    /**
     * 过滤并返回报表结果
     * @param beginTime
     * @param endTime
     * @return
     */
    private List<MutilLineReportDTO> filterResultAndBuildReports(String jobId, String beginTime, String endTime, List<RowVo> scanResults) {
        List<MutilLineReportDTO> mutilLineReportDTOs = Lists.newArrayList();

        try {
            // 先整理从hbase拿出来的数据
            Map<String,Set<String>> verticeReportTitles = Maps.newHashMap();

            LinkedHashMap<String,Map<String,List<FlinkMetricDto>>> verticeTimeToMetrics = new LinkedHashMap();

            for (RowVo rowVo : scanResults) {
                String rowKey = rowVo.getRowKey();
                String[] rowArr = rowKey.split("#");
                String rowJobId = rowArr[rowArr.length-1];
                if (!rowJobId.equals(jobId)) {
                    continue;
                }
                Map<String,String> columnDateToMap = rowVo.convertColumnDateToMap();
                if (columnDateToMap.isEmpty()) {
                    continue;
                }
                String TIME = columnDateToMap.get(MoniterContant.TIME_METRIC);
                String VERTICE_TO_METRIC = columnDateToMap.get(MoniterContant.VERTICE_TO_METRIC);
                if (StringUtils.isBlank(VERTICE_TO_METRIC)) {
                    continue;
                }

                List<FlinkVerticeMetricDto> flinkVerticeMetricDtos = JSONObject.parseArray(VERTICE_TO_METRIC,FlinkVerticeMetricDto.class);
                for (FlinkVerticeMetricDto flinkVerticeMetricDto: flinkVerticeMetricDtos) {
                    String vertice = flinkVerticeMetricDto.getName();
                    List<FlinkMetricDto> flinkMetricDtos= flinkVerticeMetricDto.getMetricDtos();

                    if(CollectionUtils.isEmpty(flinkMetricDtos)){
                        continue;
                    }

                    Set<String> metricKey = Sets.newHashSet();
                    for(FlinkMetricDto flinkMetricDto:flinkMetricDtos){
                        // 排除此指标。暂时没有什么意义，返回的格式无法解析成正确的值，以后知道什么用了可以打开。
                        if(flinkMetricDto.getId().endsWith(".latency")){
                            continue;
                        }
                        metricKey.add(flinkMetricDto.getId());
                    }

                    //放每一个vertice的数据
                    if(verticeTimeToMetrics.containsKey(vertice)){
                        Map<String,List<FlinkMetricDto>> timeToMetrics = verticeTimeToMetrics.get(vertice);
                        timeToMetrics.put(TIME,flinkMetricDtos);
                    }else{
                        Map<String,List<FlinkMetricDto>> timeToMetrics = Maps.newHashMap();
                        timeToMetrics.put(TIME,flinkMetricDtos);
                        verticeTimeToMetrics.put(vertice,timeToMetrics);
                    }

                    //放每一个vertice的reportTitles
                    if(verticeReportTitles.containsKey(vertice)){
                        Set<String> reportTitles = verticeReportTitles.get(vertice);
                        reportTitles.addAll(metricKey);
                    }else{
                        Set<String> reportTitles = Sets.newHashSet();
                        reportTitles.addAll(metricKey);
                        verticeReportTitles.put(vertice,reportTitles);
                    }
                }
            }

            for(Map.Entry<String,Map<String,List<FlinkMetricDto>>> verticeTimeToMetric:verticeTimeToMetrics.entrySet()){

                String vertice = verticeTimeToMetric.getKey();
                Map<String,List<FlinkMetricDto>> timeToMetrics = verticeTimeToMetric.getValue();
                TreeMap<String,LineReportDTO> reportDTOs = initReports(verticeReportTitles.get(vertice));

                MutilLineReportDTO mutilLineReportDTO = new MutilLineReportDTO();
                mutilLineReportDTO.setTitle(vertice);
                mutilLineReportDTO.setLineReports(Lists.newArrayList(reportDTOs.values()));

                mutilLineReportDTOs.add(mutilLineReportDTO);

                // 依次加入每分钟的数据
                for(int i=0;i<searchTimeBetweenLength.get();i++){
                    String time = DateUtil.addMinTime(beginTime,i);
                    long timeStamp = DateUtil.getCalendar(time).getTime().getTime();
                    List<FlinkMetricDto> metrics = timeToMetrics.get(time);

                    //没拿到这一分钟的数据
                    if(metrics == null){
                        // 填充-1
                        for(LineReportDTO lineReportDTO:reportDTOs.values()){
                            addDefaultGroupMetric(lineReportDTO,  lineReportDTO.getTitle(),  timeStamp,  "-1");
                        }
                    }else{
                        Map<String,String> metricsMap = Maps.newHashMap();
                        for(FlinkMetricDto metric:metrics){
                            metricsMap.put(metric.getId(),metric.getValue());
                        }
                        for(LineReportDTO lineReportDTO:reportDTOs.values()){
                           String value = metricsMap.get(lineReportDTO.getTitle());
                           if(StringUtils.isNotBlank(value)){
                               addDefaultGroupMetric(lineReportDTO,  lineReportDTO.getTitle(),  timeStamp,  value);
                           }else{
                               addDefaultGroupMetric(lineReportDTO,  lineReportDTO.getTitle(),  timeStamp,  "-1");
                           }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("FlinkReportServiceImpl getNeedData", e);
        }
        return mutilLineReportDTOs;
    }

    /**
     * 初始化报表数据
     * @param rowVo
     */
    private TreeMap<String,LineReportDTO> initReports(Set<String> reportTitles){
        TreeMap<String,LineReportDTO> lineReportDTOs = new TreeMap<String,LineReportDTO>();
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
