package com.ucar.streamsuite.moniter.web;

import com.google.common.collect.Lists;
import com.ucar.streamsuite.dao.mysql.FlinkProcessDao;
import com.ucar.streamsuite.dao.mysql.TaskDao;
import com.ucar.streamsuite.engine.po.FlinkProcessPO;
import com.ucar.streamsuite.moniter.dto.MutilLineReportDTO;
import com.ucar.streamsuite.moniter.service.FlinkReportService;
import com.ucar.streamsuite.task.po.TaskPO;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;

/**
 * Description: flinkMonitor 控制器
 * Created on 2018/1/18 下午4:33
 *
 *
 */
@Controller
@RequestMapping("/flinkMonitor")
public class FlinkMonitorController {

    @Resource
    private TaskDao taskDao;

    @Resource
    private FlinkProcessDao flinkProcessDao;

    @Autowired
    private FlinkReportService flinkReportService;

    @ResponseBody
    @RequestMapping(value = "/getReportDataByTime", method = RequestMethod.POST)
    public List<MutilLineReportDTO> getReportDataByTime(Integer taskId, String rangeTime) throws Exception{
        TaskPO taskPO = taskDao.getById(taskId);
        if(taskPO == null || taskPO.getProcessId() == null){
            return Lists.newArrayList();
        }
        if(StringUtils.isBlank(rangeTime)){
            return Lists.newArrayList();
        }
        String[] searchTime = StringUtils.split(rangeTime,",");
        Date beginTime = new Date(searchTime[0]);
        Date endTime = new Date(searchTime[1]);
        FlinkProcessPO flinkProcessPO = flinkProcessDao.getById(taskPO.getProcessId());
        if(flinkProcessPO == null){
            return Lists.newArrayList();
        }
        return flinkReportService.getReportDataByTime(flinkProcessPO.getJobId(),beginTime,endTime);
    }
}