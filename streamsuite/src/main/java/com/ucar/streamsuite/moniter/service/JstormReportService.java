package com.ucar.streamsuite.moniter.service;

import com.ucar.streamsuite.moniter.dto.LineReportDTO;

import java.util.Date;
import java.util.List;

/**
 * Description: jstorm 引擎报表服务类
 * Created on 2018/1/30 上午10:59
 *
 *
 */
public interface JstormReportService {

    List<LineReportDTO> getReportDataByTime(String topId, Date beginTime, Date endTime);

    List<String> getWorkerErrorData(String topId, Date beginTime, Date endTime);
}
