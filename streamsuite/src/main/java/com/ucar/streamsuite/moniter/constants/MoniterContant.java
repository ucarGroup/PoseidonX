package com.ucar.streamsuite.moniter.constants;

import com.alibaba.jstorm.metric.AsmWindow;
import com.ucar.streamsuite.common.constant.EngineTypeEnum;

/**
 * Description: 监控相关的常量类
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class MoniterContant {
    public static final String REPORT_DEFAULT_GROUPNAME = "defaultGroup";
    public static final String REPORT_DEFAULT_METRICVALUE = "-1";
    public static final String METRIC_COLLECT_FAILVALUE = "0";
    public static final Integer METRIC_COLLECT_CYCLE = AsmWindow.M1_WINDOW;

    //jstorm曲线查询hbase返回的最大行数 3小时*50个任务 分钟最大3小时
    public static final Integer JSTORM_REPORT_MAX_QUERY_TIME= 180;
    public static final Integer JSTORM_REPORT_MAXROW_SIZE = JSTORM_REPORT_MAX_QUERY_TIME * 50;

    //flink曲线查询hbase返回的最大行数 3小时*200个任务 分钟最大3小时
    public static final Integer FLINK_REPORT_MAX_QUERY_TIME= 180;
    public static final Integer FLINK_REPORT_MAXROW_SIZE = FLINK_REPORT_MAX_QUERY_TIME * 200;

    public static final String ROWKEY_SPLIT = "#";
    public static final String PROJECT_PREFIX= "streamsuite";
    public static final String TIMESTAMP_METRIC =  "timestamp";
    public static final String TIME_METRIC =  "time";
    public static final String VERTICE_TO_METRIC =  "verticeToMetrics";
    public static final String WORKER_ERROR_METRIC =  "worker_error";

    public static final String TASK_METRIC_JSTORM_ROWKEY = PROJECT_PREFIX + ROWKEY_SPLIT + "m" + ROWKEY_SPLIT + EngineTypeEnum.JSTORM.getDescription() + ROWKEY_SPLIT;
    public static final String TASK_METRIC_FLINK_ROWKEY = PROJECT_PREFIX + ROWKEY_SPLIT + "m" + ROWKEY_SPLIT + EngineTypeEnum.FLINK.getDescription() + ROWKEY_SPLIT;
    public static final String TASK_METRIC_HTABLE_NAME = "streamsuiteTaskMetricTable";

    public static final String WORKER_ERROR_JSTORM_ROWKEY = PROJECT_PREFIX + ROWKEY_SPLIT;
    public static final String WORKER_ERROR_HTABLE_NAME = "streamsuiteWorkerErrorTable";
}
