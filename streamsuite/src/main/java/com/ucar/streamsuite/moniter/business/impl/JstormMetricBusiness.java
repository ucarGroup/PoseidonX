package com.ucar.streamsuite.moniter.business.impl;

import backtype.storm.generated.ComponentSummary;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.WorkerSummary;
import com.alibaba.jstorm.metric.MetricType;

import com.ucar.streamsuite.moniter.dto.JstormComponentMetric;
import com.ucar.streamsuite.moniter.dto.JstormTopologyMetric;
import com.ucar.streamsuite.moniter.dto.JstormWorkerMetric;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;

public class JstormMetricBusiness {

    private static final Logger LOG = LoggerFactory.getLogger(JstormMetricBusiness.class);

    private static final DecimalFormat format = new DecimalFormat(",###.##");

    /**
     * get MetricSnapshot formatted value string
     */
    public static String getMetricValue(MetricSnapshot snapshot) {
        if (snapshot == null) return null;
        MetricType type = MetricType.parse(snapshot.get_metricType());
        switch (type) {
            case COUNTER:
                return format(snapshot.get_longValue());
            case GAUGE:
                return format(snapshot.get_doubleValue());
            case METER:
                return format(snapshot.get_m1());
            case HISTOGRAM:
                return format(snapshot.get_mean());
            default:
                return "0";
        }
    }

    public static String getMetricRawValue(MetricSnapshot snapshot) {
        MetricType type = MetricType.parse(snapshot.get_metricType());
        switch (type) {
            case COUNTER:
                return snapshot.get_longValue() + "";
            case GAUGE:
                return snapshot.get_doubleValue() + "";
            case METER:
                return snapshot.get_m1() + "";
            case HISTOGRAM:
                return snapshot.get_mean() + "";
            default:
                return "0";
        }
    }

    public static String format(double value) {
        return format.format(value);
    }

    public static String format(double value, String f){
        DecimalFormat _format = new DecimalFormat(f);
        return _format.format(value);
    }

    public static String format(long value) {
        return format.format(value);
    }

    /**
     * 获得拓扑级别的汇总数据
     * @param info
     * @param window
     * @return
     */
    public static JstormTopologyMetric buildSummaryMetrics(MetricInfo info, int window) {
        JstormTopologyMetric summaryMetric = new JstormTopologyMetric();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String metricName = extractMetricName(split_name);
                if (!metric.getValue().containsKey(window)) {
                    LOG.debug("snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);
                summaryMetric.setMetricValue(snapshot, metricName);
            }
        }
        return summaryMetric;
    }

    private static String extractMetricName(String[] strs) {
        if (strs.length < 6) return null;
        return strs[strs.length - 1];
    }

    /**
     * 获得每个组件的统计数据
     * @param info
     * @param window
     * @param componentSummaries
     * @return
     */
    public static List<JstormComponentMetric> buildComponentMetrics(MetricInfo info, int window,
                                                                  List<ComponentSummary> componentSummaries) {
        Map<String, JstormComponentMetric> componentData = new HashMap<String, JstormComponentMetric>();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String compName = extractComponentName(split_name);
                String metricName = extractMetricName(split_name);
                String parentComp = null;
                if (metricName != null && metricName.contains(".")) {
                    parentComp = metricName.split("\\.")[0];
                    metricName = metricName.split("\\.")[1];
                }

                if (!metric.getValue().containsKey(window)) {
                    LOG.debug("component snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);
                JstormComponentMetric compMetric;
                if (componentData.containsKey(compName)) {
                    compMetric = componentData.get(compName);
                } else {
                    compMetric = new JstormComponentMetric(compName);
                    componentData.put(compName, compMetric);
                }
                compMetric.setMetricValue(snapshot, parentComp, metricName);
            }
        }
        //merge sub metrics
        for (JstormComponentMetric comp : componentData.values()) {
            comp.mergeValue();
        }
        //combine the summary info into metrics
        TreeMap<String, JstormComponentMetric> ret = new TreeMap<String, JstormComponentMetric>();
        for (ComponentSummary summary : componentSummaries) {
            String compName = summary.get_name();
            JstormComponentMetric compMetric;
            if (componentData.containsKey(compName)) {
                compMetric = componentData.get(compName);
                compMetric.setParallel(summary.get_parallel());
                compMetric.setType(summary.get_type());
                compMetric.setErrors(summary.get_errors());
            } else {
                compMetric = new JstormComponentMetric(compName, summary.get_parallel(), summary.get_type());
                compMetric.setErrors(summary.get_errors());
                componentData.put(compName, compMetric);
            }
            String key = compMetric.getType() + compName;
            if (compName.startsWith("__")) {
                key = "a" + key;
            }
            compMetric.setSortedKey(key);
            ret.put(key, compMetric);
        }
        return new ArrayList<JstormComponentMetric>(ret.descendingMap().values());
    }

    private static String extractComponentName(String[] strs) {
        if (strs.length < 6) return null;
        return strs[2];
    }

    /**
     * 获得每个组件的统计数据，按照组件名统计，只拿指定组件名的
     * @param info
     * @param window
     * @param componentSummaries
     * @return
     */
    public static JstormComponentMetric buildComponentMetric(MetricInfo info, int window, String compName,
                                                           List<ComponentSummary> componentSummaries) {
        JstormComponentMetric compMetric = new JstormComponentMetric(compName);
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String componentName = extractComponentName(split_name);
                if (componentName != null && !componentName.equals(compName)) continue;

                //only handle the specific component
                String metricName = extractMetricName(split_name);
                String parentComp = null;
                if (metricName != null && metricName.contains(".")) {
                    parentComp = metricName.split("\\.")[0];
                    metricName = metricName.split("\\.")[1];
                }
                MetricSnapshot snapshot = metric.getValue().get(window);
                compMetric.setMetricValue(snapshot, parentComp, metricName);
            }
        }
        compMetric.mergeValue();
        ComponentSummary summary = null;
        for (ComponentSummary cs : componentSummaries) {
            if (cs.get_name().equals(compName)) {
                summary = cs;
                break;
            }
        }
        if (summary != null) {
            compMetric.setParallel(summary.get_parallel());
            compMetric.setType(summary.get_type());
        }
        return compMetric;
    }

    /**
     * 获得每个worker的统计数据，按照组件名统计，只拿指定组件名的
     * @param info
     * @param topology
     * @param window
     * @return
     */
    public static List<JstormWorkerMetric> buildWorkerMetrics(MetricInfo info, String topology, int window) {
        Map<String, JstormWorkerMetric> workerData = new HashMap<String, JstormWorkerMetric>();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String host = extractComponentName(split_name);
                String port = extractTaskId(split_name);
                String key = host + ":" + port;
                String metricName = extractMetricName(split_name);

                if (!metric.getValue().containsKey(window)) {
                    LOG.info("worker snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);

                JstormWorkerMetric workerMetric;
                if (workerData.containsKey(key)) {
                    workerMetric = workerData.get(key);
                } else {
                    workerMetric = new JstormWorkerMetric(host, port, topology);
                    workerData.put(key, workerMetric);
                }
                workerMetric.setMetricValue(snapshot, metricName);
            }
        }
        return new ArrayList<JstormWorkerMetric>(workerData.values());
    }

    private static String extractTaskId(String[] strs) {
        if (strs.length < 6) return null;
        return strs[3];
    }

    public static List<JstormWorkerMetric> buildWorkerMetrics(Map<String, MetricInfo> workerMetricInfo,
                                                            List<WorkerSummary> workerSummaries, String host, int window) {
        Map<String, JstormWorkerMetric> workerMetrics = new HashMap<String, JstormWorkerMetric>();
        for (MetricInfo info : workerMetricInfo.values()) {
            if (info != null) {
                for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                    String name = metric.getKey();
                    String[] split_name = name.split("@");
                    String _host = extractComponentName(split_name);
                    if (!host.equals(_host)) continue;

                    //only handle the specific host
                    String port = extractTaskId(split_name);
                    String key = host + ":" + port;
                    String metricName = extractMetricName(split_name);
                    MetricSnapshot snapshot = metric.getValue().get(window);

                    JstormWorkerMetric workerMetric;
                    if (workerMetrics.containsKey(key)) {
                        workerMetric = workerMetrics.get(key);
                    } else {
                        workerMetric = new JstormWorkerMetric(host, port);
                        workerMetrics.put(key, workerMetric);
                    }
                    workerMetric.setMetricValue(snapshot, metricName);
                }
            }
        }

        for (WorkerSummary ws : workerSummaries){
            String worker = host + ":" + ws.get_port();
            if (workerMetrics.containsKey(worker)) {
                workerMetrics.get(worker).setTopology(ws.get_topology());
            }
        }

        return new ArrayList<JstormWorkerMetric>(workerMetrics.values());
    }

    /**
     * 将字节转换为G
     * @param string
     * @return
     */
    public static String getMemNumStr(String string) {
        String result = "";
        if (StringUtils.isBlank(string)) {
            return result;
        }
        String temp = string.replace(",", "");
        if (StringUtils.isBlank(temp)) {
            return result;
        }
        try {
            long tempNum = Long.valueOf(temp);
            result = new DecimalFormat(".000").format(tempNum / Math.pow(1024, 3));
        } catch (NumberFormatException e) {
            result = "";
        }
        return result;
    }
}
