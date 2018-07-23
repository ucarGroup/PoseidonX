package com.ucar.streamsuite.moniter.business.impl;

import com.google.common.collect.Maps;
import com.ucar.streamsuite.common.util.LruHashMap;
import com.ucar.streamsuite.moniter.constants.MoniterContant;
import com.ucar.streamsuite.moniter.dto.LineReportDTO;
import com.ucar.streamsuite.moniter.dto.LineReportTimelineDTO;
import com.ucar.streamsuite.moniter.dto.MetricValueDTO;
import org.assertj.core.util.Lists;

import java.util.List;
import java.util.Map;

public class MetricReportContainer {

    /**
     * 存放报表数据的容器。
     */
    private static Map<String,Map<String,LineReportDTO>> reportContainer = new LruHashMap<String, Map<String,LineReportDTO>>(50);

    public static void removeReports(String topology){
        reportContainer.remove(topology);
    }

    public static void addDefaultGroupMetric(String groupValue, String metricName, long metricTimeStamp,String metriValue){
        Map<String,String> metricDatas = Maps.newHashMap();
        metricDatas.put(MoniterContant.REPORT_DEFAULT_GROUPNAME, metriValue);
        addMetric(groupValue, metricName, metricTimeStamp,  metricDatas);
    }

    public static LineReportDTO getReport(String topology,String title){
        Map<String,LineReportDTO> lineReports = reportContainer.get(topology);
        if(lineReports != null){
            LineReportDTO lineReport = lineReports.get(title);
            if(lineReport!=null){
                return lineReport;
            }
        }
        LineReportDTO lineReport = new LineReportDTO(title);
        MetricReportContainer.addReport(topology,lineReport);
        return lineReport;
    }

    public static List<LineReportDTO> getReports(String topology){
        Map<String,LineReportDTO> lineReports = reportContainer.get(topology);
        if(lineReports == null){
            return Lists.newArrayList();
        }
        return Lists.newArrayList(lineReports.values());
    }

    private static void addReport(String topology,LineReportDTO lineReport){
        Map<String,LineReportDTO> lineReports = reportContainer.get(topology);
        if(lineReports == null){
            lineReports = Maps.newHashMap();
        }
        lineReports.put(lineReport.getTitle(),lineReport);
        reportContainer.put(topology,lineReports);
    }

    /**
     * 将指标的值放入缓存容器
     * @param topId
     * @param currentTime
     */
    private static void addMetric(String topId, String reportTitle, long metricTimeStamp,  Map<String, String> currentTimeDatas) {
        LineReportDTO lineReport = MetricReportContainer.getReport(topId,reportTitle);
        for(Map.Entry<String,String> currentTimeData:currentTimeDatas.entrySet()){
            String group = currentTimeData.getKey();
            String metricValue = currentTimeData.getValue();
            LineReportTimelineDTO timeline = lineReport.getTimeLineByGroup(group);
            if(timeline == null){
                timeline = new LineReportTimelineDTO(currentTimeData.getKey());
                lineReport.addTimeLine(timeline);
            }
            timeline.append(new MetricValueDTO(metricTimeStamp,metricValue));
        }
    }

}
