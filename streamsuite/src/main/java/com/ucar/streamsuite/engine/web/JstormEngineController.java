package com.ucar.streamsuite.engine.web;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.ucar.streamsuite.common.constant.*;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.util.*;

import com.ucar.streamsuite.dao.mysql.CqlDao;
import com.ucar.streamsuite.dao.mysql.JstormProcessDao;
import com.ucar.streamsuite.dao.mysql.TaskDao;
import com.ucar.streamsuite.engine.dto.*;

import com.ucar.streamsuite.engine.po.JstormProcessPO;

import com.ucar.streamsuite.moniter.service.JstormReportService;
import com.ucar.streamsuite.task.po.TaskPO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;


/**
 * Description: jstorm引擎控制器
 * Created on 2018/1/18 下午4:33
 *
 */
@Controller
@RequestMapping("/jstormEngine")
public class JstormEngineController {

    @Resource
    private JstormProcessDao jstormProcessDao;

    @Autowired
    private JstormReportService jstormReportService;

    @Resource
    private TaskDao taskDao;

    @Resource
    private CqlDao cqlDao;

    @ResponseBody
    @RequestMapping(value = "/getClusterDetailByTaskId", method = RequestMethod.POST)
    public List<JstormClusterDTO> getClusterDetailByTaskId(Integer taskId) throws Exception{
        List<JstormClusterDTO> rs = Lists.newArrayList();
        TaskPO taskPO = taskDao.getById(taskId);
        if(taskPO == null || taskPO.getProcessId() == null){
            return Lists.newArrayList();
        }
        JstormProcessPO jstormProcessPO = jstormProcessDao.getById(taskPO.getProcessId());
        if(jstormProcessPO == null){
            return Lists.newArrayList();
        }
        JstormTaskDetailDTO jstormTaskDetailDTO = JSONObject.parseObject(jstormProcessPO.getTaskDetail(), JstormTaskDetailDTO.class);
        if(jstormProcessPO == null){
            return Lists.newArrayList();
        }

        JstormClusterDTO jstormClusterDTO = jstormTaskDetailDTO.getJstormClusterDTO();
        if(jstormTaskDetailDTO.getSavepointType() == JstormTopologySavePointEnum.APP_ERROR.getValue()){
            jstormClusterDTO.setAppState("INACTIVE");
        }
        if(jstormTaskDetailDTO.getSavepointType() == JstormTopologySavePointEnum.NIMBUS_CHANGE.getValue() || jstormTaskDetailDTO.getSavepointType() == JstormTopologySavePointEnum.NIMBUS_ERROR.getValue()){
            if(CollectionUtils.isNotEmpty(jstormClusterDTO.getNimbusInfos()) && jstormClusterDTO.getNimbusInfos().get(0)!=null){
                jstormClusterDTO.getNimbusInfos().get(0).setContainerStats("INACTIVE");
            }
        }

        // 恢复后仍失败，全Inactive
        if(jstormTaskDetailDTO.getSavepointType() == JstormTopologySavePointEnum.ALL_ERROR.getValue()){
            jstormClusterDTO.setAppState("INACTIVE");
            if(CollectionUtils.isNotEmpty(jstormClusterDTO.getNimbusInfos()) && jstormClusterDTO.getNimbusInfos().get(0)!=null){
                jstormClusterDTO.getNimbusInfos().get(0).setContainerStats("INACTIVE");
            }
            if(CollectionUtils.isNotEmpty(jstormClusterDTO.getSupervisorInfos())){
                for(JstormClusterSupervisoDTO jstormClusterSupervisoDTO : jstormClusterDTO.getSupervisorInfos()){
                    jstormClusterSupervisoDTO.setContainerStats("INACTIVE");
                }
            }
        }

        //处理正常停止情况的状态显示问题
        if(taskPO.getTaskStatus() == TaskStatusEnum.STOP.getValue() && jstormTaskDetailDTO.getSavepointType() == JstormTopologySavePointEnum.COMMON.getValue()){
            jstormClusterDTO.setAppState("STOPED");
            if(CollectionUtils.isNotEmpty(jstormClusterDTO.getNimbusInfos()) && jstormClusterDTO.getNimbusInfos().get(0)!=null){
                jstormClusterDTO.getNimbusInfos().get(0).setContainerStats("STOPED");
            }
            if(CollectionUtils.isNotEmpty(jstormClusterDTO.getSupervisorInfos())){
                for(JstormClusterSupervisoDTO jstormClusterSupervisoDTO : jstormClusterDTO.getSupervisorInfos()){
                    jstormClusterSupervisoDTO.setContainerStats("STOPED");
                }
            }
        }
        rs.add(jstormClusterDTO);
        return rs;
    }

    @ResponseBody
    @RequestMapping(value = "/getTopologyDetailByTaskId", method = RequestMethod.POST)
    public List<JstormTopologyDTO> getTopologyDetailByTaskId(Integer taskId) throws Exception{
        List<JstormTopologyDTO> rs = Lists.newArrayList();

        TaskPO taskPO = taskDao.getById(taskId);
        if(taskPO == null || taskPO.getProcessId() == null){
            return Lists.newArrayList();
        }
        JstormProcessPO jstormProcessPO = jstormProcessDao.getById(taskPO.getProcessId());
        if(jstormProcessPO == null){
            return Lists.newArrayList();
        }
        JstormTaskDetailDTO jstormTaskDetailDTO = JSONObject.parseObject(jstormProcessPO.getTaskDetail(), JstormTaskDetailDTO.class);
        if(jstormProcessPO == null){
            return Lists.newArrayList();
        }
        JstormTopologyDTO jstormTopologyDTO = jstormTaskDetailDTO.getJstormTopologyDTO();
        if(jstormTaskDetailDTO.getSavepointType() == JstormTopologySavePointEnum.TOP_ERROR.getValue()){
            jstormTopologyDTO.setStatus("INACTIVE");
        }

        for(JstormCompenentDTO jstormCompenentDTO : jstormTopologyDTO.getCompenentInfos()){

            Integer tasksActiveCount = jstormCompenentDTO.getTasksActiveCount() == null?0:jstormCompenentDTO.getTasksActiveCount();
            Integer tasksStartingCount = jstormCompenentDTO.getTasksStartingCount()== null?0:jstormCompenentDTO.getTasksStartingCount();
            Integer tasksTotal = jstormCompenentDTO.getTasksTotal()== null?0:jstormCompenentDTO.getTasksTotal();

            jstormCompenentDTO.setTaskInfo(tasksStartingCount + "/" + tasksActiveCount +"/" + tasksTotal);
        }

        //处理正常停止情况的状态显示问题
        if(taskPO.getTaskStatus() == TaskStatusEnum.STOP.getValue() && jstormTaskDetailDTO.getSavepointType() == JstormTopologySavePointEnum.COMMON.getValue()){
            jstormTopologyDTO.setStatus("STOPED");
            for(JstormCompenentDTO jstormCompenentDTO : jstormTopologyDTO.getCompenentInfos()){
                jstormCompenentDTO.setTaskInfo( "0/0/0");
            }
        }
        rs.add(jstormTopologyDTO);
        return rs;
    }

    @ResponseBody
    @RequestMapping(value = "/getTopologyExceptionByTaskId", method = RequestMethod.POST)
    public  JstormTopologyExceptionDTO  getTopologyExceptionByTaskId(Integer taskId) throws Exception{
        JstormTopologyExceptionDTO jstormTopologyExceptionDTO = new JstormTopologyExceptionDTO();
        jstormTopologyExceptionDTO.setRootException(Lists.<String>newArrayList());
        jstormTopologyExceptionDTO.setWorkerExceptions(Lists.<String>newArrayList());

        TaskPO taskPO = taskDao.getById(taskId);
        if(taskPO == null || taskPO.getProcessId() == null){
            return jstormTopologyExceptionDTO;
        }
        JstormProcessPO jstormProcessPO = jstormProcessDao.getById(taskPO.getProcessId());
        if(jstormProcessPO == null){
            return jstormTopologyExceptionDTO;
        }

        List<String> rootExceptions = JSONObject.parseArray(taskPO.getErrorInfo(),String.class);
        if(CollectionUtils.isNotEmpty(rootExceptions)){
            jstormTopologyExceptionDTO.setRootException(rootExceptions);
        }

        Date end = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        List<String> workerErrors = jstormReportService.getWorkerErrorData(jstormProcessPO.getTopId(),dateFormat.parse(DateUtil.getDate(-7)),end);
        if(CollectionUtils.isNotEmpty(workerErrors)){
            jstormTopologyExceptionDTO.setWorkerExceptions(workerErrors);
        }
        return jstormTopologyExceptionDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/listProcessByTaskId", method = RequestMethod.POST)
    public PageResultDTO listProcessByTaskId(ProcessPageQueryDTO processPageQueryDTO) throws Exception{

        StreamUtil.dealPageQuery(processPageQueryDTO);

        PageResultDTO pageResultDTO = new PageResultDTO();

        List<ProcessDTO> rsDtos= Lists.newArrayList();
        List<JstormProcessPO> jstormProcessPOs= jstormProcessDao.pageQuery(processPageQueryDTO);

        if(CollectionUtils.isNotEmpty(jstormProcessPOs)){
            for(JstormProcessPO jstormProcessPO:jstormProcessPOs){
                try{
                    ProcessDTO dto = new ProcessDTO();
                    dto.setRowId(UUIDUtil.generate());

                    JstormTaskConfigDTO taskConfigDTO = JSONObject.parseObject(jstormProcessPO.getTaskConfig(), JstormTaskConfigDTO.class);
                    if(taskConfigDTO==null){
                        continue;
                    }

                    List<String> configs = Lists.newArrayList();
                    if(taskConfigDTO.getTaskCqlId() == null){
                        configs.add("运行API文件：" + taskConfigDTO.getProjectJarPath());
                        configs.add("mainClass：" + taskConfigDTO.getClassPath());
                        List<String> zkConnetcionStrings = Lists.newArrayList();
                        for (String zkAddres : taskConfigDTO.getJstormZkHost()) {
                            zkConnetcionStrings.add(zkAddres + ":" + taskConfigDTO.getJstormZkPort());
                        }
                        String zkConnetcionString = StringUtils.join(zkConnetcionStrings,",");
                        configs.add("zk信息：" + zkConnetcionString);
                        configs.add("worker：" + taskConfigDTO.getWorkerMem() + " * " + taskConfigDTO.getWorkerNum());
                        configs.add("jstorm 版本包：" + taskConfigDTO.getJstormJarPath());
                        configs.add("jstorm AM版本包：" + taskConfigDTO.getYarnAmJarPath());
                    }else{
                        configs.add("运行CQL脚本：" + taskConfigDTO.getTaskCql());
                        List<String> zkConnetcionStrings = Lists.newArrayList();
                        for (String zkAddres : taskConfigDTO.getJstormZkHost()) {
                            zkConnetcionStrings.add(zkAddres + ":" + taskConfigDTO.getJstormZkPort());
                        }
                        String zkConnetcionString = StringUtils.join(zkConnetcionStrings,",");
                        configs.add("zk信息：" + zkConnetcionString);
                        configs.add("worker：" + taskConfigDTO.getWorkerMem() + " * " + taskConfigDTO.getWorkerNum());
                        configs.add("jstorm 版本包：" + taskConfigDTO.getJstormJarPath());
                        configs.add("jstorm AM版本包：" + taskConfigDTO.getYarnAmJarPath());
                    }

                    dto.setStartTime(jstormProcessPO.getStartTime());
                    dto.setTaskConfig(configs);
                    dto.setLogMessage(null);
                    dto.setYarnAppId(jstormProcessPO.getYarnAppId());
                    dto.setType(TaskSubmitTypeEnum.getDescription(jstormProcessPO.getSubmitType()));
                    dto.setResult(jstormProcessPO.getSubmitResult());
                    rsDtos.add(dto);
                }catch (Exception e){
                }
            }
        }
        //查询列表
        pageResultDTO.setList(rsDtos);
        //查询总数
        pageResultDTO.setCount(jstormProcessDao.queryCount(processPageQueryDTO));
        pageResultDTO.setCurrentPage(processPageQueryDTO.getPageNum());
        return pageResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/getContainerMem", method = RequestMethod.POST)
    public String getContainerMem() {
        return YarnClientProxy.getConatinerMaxMem() + "," +  YarnClientProxy.getConatinerMinMem();
    }
}