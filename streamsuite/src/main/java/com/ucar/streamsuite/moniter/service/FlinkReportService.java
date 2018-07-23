package com.ucar.streamsuite.moniter.service;

import com.ucar.streamsuite.moniter.dto.LineReportDTO;
import com.ucar.streamsuite.moniter.dto.MutilLineReportDTO;

import java.util.Date;
import java.util.List;

/**
 * Description: flink 引擎报表服务类
 * Created on 2018/1/30 上午10:59
 *
 *
 */
public interface FlinkReportService {

    List<MutilLineReportDTO> getReportDataByTime(String jobId, Date beginTime, Date endTime);

}
