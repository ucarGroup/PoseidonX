package com.ucar.streamsuite.moniter.web;

import com.google.common.collect.Lists;
import com.ucar.streamsuite.dao.mysql.JstormProcessDao;
import com.ucar.streamsuite.dao.mysql.TaskDao;
import com.ucar.streamsuite.engine.po.JstormProcessPO;
import com.ucar.streamsuite.moniter.business.impl.MetricReportContainer;
import com.ucar.streamsuite.moniter.dto.LineReportDTO;
import com.ucar.streamsuite.moniter.service.JstormReportService;
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
 * Description: jstormMonitor 控制器
 * Created on 2018/1/18 下午4:33
 *
 *
 */
@Controller
@RequestMapping("/jstormMonitor")
public class JstormMonitorController {

    @Resource
    private TaskDao taskDao;

    @Resource
    private JstormProcessDao jstormProcessDao;

    @Autowired
    private JstormReportService jstormReportService;

    @ResponseBody
    @RequestMapping(value = "/getReportRecentDataByTaskId", method = RequestMethod.POST)
    public List<LineReportDTO> getReportRecentDataByTaskId(Integer taskId) throws Exception{
        TaskPO taskPO = taskDao.getById(taskId);
        if(taskPO == null || taskPO.getProcessId() == null){
            return Lists.newArrayList();
        }
        JstormProcessPO jstormProcessPO = jstormProcessDao.getById(taskPO.getProcessId());
        if(jstormProcessPO == null){
            return Lists.newArrayList();
        }
        return MetricReportContainer.getReports(jstormProcessPO.getTopId());
    }

    @ResponseBody
    @RequestMapping(value = "/getReportDataByTime", method = RequestMethod.POST)
    public List<LineReportDTO> getReportDataByTime(Integer taskId,String rangeTime) throws Exception{
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
        JstormProcessPO jstormProcessPO = jstormProcessDao.getById(taskPO.getProcessId());
        if(jstormProcessPO == null){
            return Lists.newArrayList();
        }
        return jstormReportService.getReportDataByTime(jstormProcessPO.getTopId(),beginTime,endTime);
    }

    @ResponseBody
    @RequestMapping(value = "/getWorkerErrorData", method = RequestMethod.POST)
    public List<String> getWorkerErrorData(Integer taskId,String rangeTime) throws Exception{
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
        JstormProcessPO jstormProcessPO = jstormProcessDao.getById(taskPO.getProcessId());
        if(jstormProcessPO == null){
            return Lists.newArrayList();
        }
        return jstormReportService.getWorkerErrorData(jstormProcessPO.getTopId(),beginTime,endTime);
    }

}