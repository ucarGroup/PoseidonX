package com.ucar.streamsuite.task.web;

import com.google.common.collect.Sets;

import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.ConfigKeyEnum;
import com.ucar.streamsuite.common.constant.UserRoleEnum;
import com.ucar.streamsuite.common.dto.OperResultDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;

import com.ucar.streamsuite.common.filter.WebContextHolder;

import com.ucar.streamsuite.task.dto.TaskDTO;
import com.ucar.streamsuite.task.dto.TaskPageQueryDTO;

import com.ucar.streamsuite.task.dto.TaskStartTimeLineDTO;
import com.ucar.streamsuite.task.po.TaskPO;
import com.ucar.streamsuite.task.service.TaskService;

import com.ucar.streamsuite.user.po.UserPO;
import org.apache.commons.lang.StringUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Set;

/**
 * Description: 任务控制器
 * Created on 2018/1/18 下午4:33
 *
 */
@Controller
@RequestMapping("/task/task")
public class TaskController {

    @Autowired
    private TaskService taskService;

    @ResponseBody
    @RequestMapping(value = "/getTaskByUser", method = RequestMethod.POST)
    public List<TaskDTO> getTaskByUser(Integer engineType) {
        UserPO userPO = WebContextHolder.getLoginUser();
        String queryUserName = "";
        if(userPO.getUserRole() != UserRoleEnum.SUPER_ADMIN.ordinal()){
            queryUserName = userPO.getUserName();
        }
        TaskPageQueryDTO taskPageQueryDTO = new TaskPageQueryDTO();
        taskPageQueryDTO.setPageNum(1);
        taskPageQueryDTO.setPageSize(Integer.MAX_VALUE);
        taskPageQueryDTO.setEngineTypeCondition(engineType);
        return taskService.pageQuery(taskPageQueryDTO).getList();
    }

    @ResponseBody
    @RequestMapping(value = "/list", method = RequestMethod.POST)
    public PageResultDTO list(TaskPageQueryDTO taskPageQueryDTO) {
        return taskService.pageQuery(taskPageQueryDTO);
    }

    @ResponseBody
    @RequestMapping(value = "/queryById", method = RequestMethod.POST)
    public TaskDTO queryById(Integer id) {
        if(id == null){
            return new TaskDTO();
        }
        TaskPO taskPO = taskService.getTaskById(id);
        return taskService.convertPoToDto(taskPO,false);
    }

    @ResponseBody
    @RequestMapping(value = "/showById", method = RequestMethod.POST)
    public TaskDTO showById(Integer id) {
        if(id == null){
            return new TaskDTO();
        }
        TaskPO taskPO = taskService.getTaskById(id);
        return taskService.convertPoToDto(taskPO,true);
    }

    @ResponseBody
    @RequestMapping(value = "/aduit", method = RequestMethod.POST)
    public OperResultDTO aduit(Integer id,Integer aduitStatus) {
        OperResultDTO operResultDTO= new OperResultDTO();
        operResultDTO.setResult(true);
        if(id == null || aduitStatus == null){
            operResultDTO.setErrMsg("提交审核时，参数为空！");
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        try{
            taskService.aduit(id,aduitStatus);
        }catch (Exception e){
            operResultDTO.setErrMsg(e.getMessage());
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        return operResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/start", method = RequestMethod.POST)
    public OperResultDTO start(Integer id) {
        OperResultDTO operResultDTO= new OperResultDTO();
        operResultDTO.setResult(true);
        if(id == null){
            operResultDTO.setErrMsg("开始任务时，参数为空！");
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        try{
            taskService.beginTask(id);
        }catch (Exception e){
            operResultDTO.setErrMsg(e.getMessage());
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        return operResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/getStartTimeLine", method = RequestMethod.POST)
    public TaskStartTimeLineDTO getStartTimeLine(Integer id) {
        return taskService.getStartTimeLineByTaskId(id);
    }

    @ResponseBody
    @RequestMapping(value = "/stop", method = RequestMethod.POST)
    public OperResultDTO stop(Integer id) {
        OperResultDTO operResultDTO= new OperResultDTO();
        operResultDTO.setResult(true);
        if(id == null){
            operResultDTO.setErrMsg("停止任务时，参数为空！");
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        try{
            taskService.stopTask(id);
        }catch (Exception e){
            operResultDTO.setErrMsg(e.getMessage());
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        return operResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    public OperResultDTO delete(Integer id) {
        OperResultDTO operResultDTO= new OperResultDTO();
        operResultDTO.setResult(true);
        if(id == null){
            operResultDTO.setErrMsg("删除任务时，参数为空！");
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        try{
            taskService.deleteTask(id);
        }catch (Exception e){
            operResultDTO.setErrMsg(e.getMessage());
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        return operResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public OperResultDTO save(TaskDTO taskDTO) {
        OperResultDTO operResultDTO= new OperResultDTO();
        operResultDTO.setResult(true);
        try{
            taskService.saveTask(taskDTO);
        }catch (Exception e){
            operResultDTO.setErrMsg(e.getMessage());
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        return operResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/update", method = RequestMethod.POST)
    public OperResultDTO update(TaskDTO taskDTO) {
        OperResultDTO operResultDTO= new OperResultDTO();
        operResultDTO.setResult(true);
        try{
            taskService.updateTask(taskDTO);
        }catch (Exception e){
            operResultDTO.setErrMsg(e.getMessage());
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        return operResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/getConfigZkInfo", method = RequestMethod.POST)
    public String getConfigZkInfo() {
        String zkHosts = ConfigProperty.getConfigValue(ConfigKeyEnum.ZK_HOST);
        String zkPort = ConfigProperty.getConfigValue(ConfigKeyEnum.ZK_PORT);
        if(StringUtils.isBlank(zkHosts) || StringUtils.isBlank(zkPort)){
            return "";
        }
        Set<String> zkAddress= Sets.newHashSet();
        for(String jstormZkHost: StringUtils.split(zkHosts,",")){
            zkAddress.add( jstormZkHost + ":" + zkPort);
        }
        return StringUtils.join(zkAddress,",");
    }


}