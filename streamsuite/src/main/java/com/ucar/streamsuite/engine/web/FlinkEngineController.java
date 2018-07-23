package com.ucar.streamsuite.engine.web;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.ucar.streamsuite.common.constant.FlinkJobSavePointEnum;
import com.ucar.streamsuite.common.constant.JstormTopologySavePointEnum;
import com.ucar.streamsuite.common.constant.TaskStatusEnum;
import com.ucar.streamsuite.common.constant.TaskSubmitTypeEnum;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.util.DateUtil;
import com.ucar.streamsuite.common.util.StreamUtil;
import com.ucar.streamsuite.common.util.UUIDUtil;

import com.ucar.streamsuite.dao.mysql.FlinkProcessDao;
import com.ucar.streamsuite.dao.mysql.TaskDao;
import com.ucar.streamsuite.engine.dto.*;
import com.ucar.streamsuite.engine.po.FlinkProcessPO;
import com.ucar.streamsuite.task.po.TaskPO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

/**
 * Description: flink引擎控制器
 * Created on 2018/1/18 下午4:33
 *
 */
@Controller
@RequestMapping("/flinkEngine")
public class FlinkEngineController {

    @Resource
    private FlinkProcessDao flinkProcessDao;

    @Resource
    private TaskDao taskDao;

    @ResponseBody
    @RequestMapping(value = "/getJobManagerConfigByTaskId", method = RequestMethod.POST)
    public List<FlinkJobManagerConfigDTO> getJobManagerConfigByTaskId(Integer taskId) throws Exception{
        FlinkTaskDetailDTO flinkTaskDetailDTO = getFlinkTaskDetailDto(taskId);
        if (flinkTaskDetailDTO == null){
            return Lists.newArrayList();
        }
        List<FlinkJobManagerConfigDTO> jobManagerConfigDTOs = JSONObject.parseArray(flinkTaskDetailDTO.getJobmanagerConfig(),FlinkJobManagerConfigDTO.class);
        if(jobManagerConfigDTOs == null){
            return Lists.newArrayList();
        }
        return jobManagerConfigDTOs;
    }

    @ResponseBody
    @RequestMapping(value = "/getTaskManagersByTaskId", method = RequestMethod.POST)
    public List<FlinkTaskManagerDTO>  getTaskManagersByTaskId(Integer taskId) throws Exception{
        List<FlinkTaskManagerDTO> taskManagerDTOs = Lists.newArrayList();
        FlinkTaskDetailDTO flinkTaskDetailDTO = getFlinkTaskDetailDto(taskId);
        if (flinkTaskDetailDTO == null){
            return taskManagerDTOs;
        }
        if(CollectionUtils.isEmpty(flinkTaskDetailDTO.getTaskManagers())){
            return taskManagerDTOs;
        }
        for(String taskManager:flinkTaskDetailDTO.getTaskManagers()){
            JSONObject taskManagerJsonObject = JSONObject.parseObject(taskManager);
            List<FlinkTaskManagerDTO> flinkTaskManagerDTOs = JSONObject.parseArray(taskManagerJsonObject.getString("taskmanagers"),FlinkTaskManagerDTO.class);
            if(CollectionUtils.isNotEmpty(flinkTaskManagerDTOs)){
                FlinkTaskManagerDTO flinkTaskManagerDTO = flinkTaskManagerDTOs.get(0);

                flinkTaskManagerDTO.setLastHeartbeat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(flinkTaskManagerDTO.getTimeSinceLastHeartbeat()));
                flinkTaskManagerDTO.setPhysicalMemoryShow( formetFileSize( flinkTaskManagerDTO.getPhysicalMemory()));
                flinkTaskManagerDTO.setFreeMemoryShow( formetFileSize( flinkTaskManagerDTO.getFreeMemory()));
                flinkTaskManagerDTO.setManagedMemoryShow( formetFileSize( flinkTaskManagerDTO.getManagedMemory()));
                flinkTaskManagerDTO.setPhysicalMemoryShow( formetFileSize( flinkTaskManagerDTO.getPhysicalMemory()));
                taskManagerDTOs.add(flinkTaskManagerDTO);
            }
        }
        return taskManagerDTOs;
    }

    @ResponseBody
    @RequestMapping(value = "/getJobExceptionByTaskId", method = RequestMethod.POST)
    public  FlinkJobExceptionDTO  getJobExceptionByTaskId(Integer taskId) throws Exception{
        FlinkJobExceptionDTO flinkJobExceptionDTO = new FlinkJobExceptionDTO();
        flinkJobExceptionDTO.setRootExceptionShow(Lists.<String>newArrayList());
        flinkJobExceptionDTO.setAllExceptions(Lists.<FlinkJobExceptionDetailDTO>newArrayList());

        FlinkTaskDetailDTO flinkTaskDetailDTO = getFlinkTaskDetailDto(taskId);
        if (flinkTaskDetailDTO == null){
            return flinkJobExceptionDTO;
        }
        String jobException =  flinkTaskDetailDTO.getExceptions();
        if(StringUtils.isBlank(jobException)){
            return flinkJobExceptionDTO;
        }
        flinkJobExceptionDTO = JSONObject.parseObject(jobException,FlinkJobExceptionDTO.class);
        if(flinkJobExceptionDTO == null){
            return flinkJobExceptionDTO;
        }

        if(flinkJobExceptionDTO.getRootException() == null){
            flinkJobExceptionDTO.setRootExceptionShow(Lists.<String>newArrayList());
        }else{
            flinkJobExceptionDTO.setRootExceptionShow(Arrays.asList(StringUtils.split(flinkJobExceptionDTO.getRootException(),"\n\t")));
        }

        if(CollectionUtils.isEmpty(flinkJobExceptionDTO.getAllExceptions())){
            flinkJobExceptionDTO.setAllExceptions(Lists.<FlinkJobExceptionDetailDTO>newArrayList());
        }else{
            Integer i = 1;
            for(FlinkJobExceptionDetailDTO flinkJobExceptionDetailDTO :flinkJobExceptionDTO.getAllExceptions()){
                flinkJobExceptionDetailDTO.setId(i++);
                flinkJobExceptionDetailDTO.setExceptionShow(Arrays.asList(StringUtils.split(flinkJobExceptionDetailDTO.getException(),"\n\t")));
            }
        }
        return flinkJobExceptionDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/getJobDetailByTaskId", method = RequestMethod.POST)
    public List<FlinkJobDTO> getJobDetailByTaskId(Integer taskId) throws Exception{
        List<FlinkJobDTO> rs = Lists.newArrayList();

        TaskPO taskPO = taskDao.getById(taskId);
        if(taskPO == null || taskPO.getProcessId() == null){
            return rs;
        }

        FlinkProcessPO flinkProcessPO = flinkProcessDao.getById(taskPO.getProcessId());
        if(flinkProcessPO == null || StringUtils.isBlank(flinkProcessPO.getTaskDetail()) || StringUtils.isBlank(flinkProcessPO.getYarnAppId())){
            return rs;
        }

        FlinkTaskDetailDTO taskDetailDTO = getFlinkTaskDetailDto(taskId);
        if(taskDetailDTO == null){
            return rs;
        }

        FlinkJobDTO flinkJobDTO = JSONObject.parseObject(taskDetailDTO.getJobDetail(), FlinkJobDTO.class);
        flinkJobDTO.setAppStatus("RUNNING");
        flinkJobDTO.setAppId(flinkProcessPO.getYarnAppId());
        if(taskDetailDTO.getSavepointType() == FlinkJobSavePointEnum.APP_ERROR.getValue()){
            flinkJobDTO.setAppStatus("INACTIVE");
        }
        if(taskDetailDTO.getSavepointType() == FlinkJobSavePointEnum.JOB_ERROR.getValue()){
            flinkJobDTO.setState("INACTIVE");
        }

        JSONObject appOverviewJsonObject = JSONObject.parseObject(taskDetailDTO.getAppOverview());
        flinkJobDTO.setTaskmanagers(appOverviewJsonObject.getInteger("taskmanagers"));
        flinkJobDTO.setSlotsTotal( appOverviewJsonObject.getInteger("slots-total"));
        flinkJobDTO.setSlotsAvailable(appOverviewJsonObject.getInteger("slots-available"));
        flinkJobDTO.setFlinkVersion(appOverviewJsonObject.getString("flink-version"));
        flinkJobDTO.setStartTimeShow(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(flinkJobDTO.getStartTime()));
        Long duration = flinkJobDTO.getDuration().longValue()/1000;
        flinkJobDTO.setUptime(DateUtil.prettyUptime(duration.intValue()));

        for(FlinkJobVerticeDTO flinkJobVerticeDTO:flinkJobDTO.getVertices()){
            for(String verticesJson: taskDetailDTO.getVertices()){
                JSONObject verticesJSONObject = JSONObject.parseObject(verticesJson);
                if(flinkJobVerticeDTO.getId().equals(verticesJSONObject.getString("id"))){
                    List<FlinkJobVerticeSubTaskDTO> flinkJobVerticeSubTaskDTOs= JSONObject.parseArray(verticesJSONObject.getString("subtasks"),FlinkJobVerticeSubTaskDTO.class);
                    flinkJobVerticeDTO.setSubTasks(flinkJobVerticeSubTaskDTOs);
                }
            }
            Long flinkJobVerticeDuration = flinkJobVerticeDTO.getDuration().longValue()/1000;
            flinkJobVerticeDTO.setUptime(DateUtil.prettyUptime(flinkJobVerticeDuration.intValue()));
            flinkJobVerticeDTO.setStartTimeShow(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(flinkJobVerticeDTO.getStartTime()));
        }

        // 恢复后仍失败，全Inactive
        if(taskDetailDTO.getSavepointType() == FlinkJobSavePointEnum.ALL_ERROR.getValue()){
            flinkJobDTO.setAppStatus("INACTIVE");
            flinkJobDTO.setState("INACTIVE");
            for(FlinkJobVerticeDTO flinkJobVerticeDTO:flinkJobDTO.getVertices()){
                flinkJobVerticeDTO.setStatus("INACTIVE");
            }
        }

        //处理正常停止情况的状态显示问题
        if(taskPO.getTaskStatus() == TaskStatusEnum.STOP.getValue() && taskDetailDTO.getSavepointType() == FlinkJobSavePointEnum.COMMON.getValue()){
            flinkJobDTO.setAppStatus("STOPED");
            flinkJobDTO.setState("STOPED");
            for(FlinkJobVerticeDTO flinkJobVerticeDTO:flinkJobDTO.getVertices()){
                flinkJobVerticeDTO.setStatus("STOPED");
            }
        }
        return Lists.newArrayList(flinkJobDTO);
    }

    @ResponseBody
    @RequestMapping(value = "/listProcessByTaskId", method = RequestMethod.POST)
    public PageResultDTO listProcessByTaskId(ProcessPageQueryDTO processPageQueryDTO) throws Exception{

        StreamUtil.dealPageQuery(processPageQueryDTO);

        PageResultDTO pageResultDTO = new PageResultDTO();

        List<ProcessDTO> rsDtos= Lists.newArrayList();
        List<FlinkProcessPO> flinkProcessPOs= flinkProcessDao.pageQuery(processPageQueryDTO);

        if(CollectionUtils.isNotEmpty(flinkProcessPOs)){
            for(FlinkProcessPO flinkProcessPO:flinkProcessPOs){
                try{
                    ProcessDTO dto = new ProcessDTO();
                    dto.setRowId(UUIDUtil.generate());

                    FlinkTaskConfigDTO taskConfigDTO = JSONObject.parseObject(flinkProcessPO.getTaskConfig(), FlinkTaskConfigDTO.class);
                    if(taskConfigDTO==null){
                        continue;
                    }

                    List<String> configs = Lists.newArrayList();
                    if(taskConfigDTO.getTaskCqlId() == null) {
                        configs.add("运行API文件：" + taskConfigDTO.getProjectJarPath());
                        configs.add("mainClass：" + taskConfigDTO.getClassPath());
                        configs.add("JobManagerMem：" + taskConfigDTO.getJobMangerMem());
                        configs.add("TaskManager：" + taskConfigDTO.getTaskMangerMem() + " * " + taskConfigDTO.getTaskMangerNum());
                        configs.add("Slots：" + taskConfigDTO.getSlots());
                        configs.add("Parallelism：" + (taskConfigDTO.getParallelism()!=null?taskConfigDTO.getParallelism():""));
                        configs.add("用户参数：" + taskConfigDTO.getCustomParams());
                    }else{
                        configs.add("JobManagerMem：" + taskConfigDTO.getJobMangerMem());
                        configs.add("TaskManager：" + taskConfigDTO.getTaskMangerMem() + " * " + taskConfigDTO.getTaskMangerNum());
                        configs.add("Slots：" + taskConfigDTO.getSlots());
                        configs.add("Parallelism：" + (taskConfigDTO.getParallelism()!=null?taskConfigDTO.getParallelism():""));
                        configs.add("运行CQL脚本：" + taskConfigDTO.getTaskCql());
                    }

                    List<String> logMessages = Lists.newArrayList();
                    if(StringUtils.isNotBlank(flinkProcessPO.getJobLogMessage())){
                        logMessages = Lists.newArrayList(StringUtils.split(flinkProcessPO.getJobLogMessage(),"||||"));
                    }

                    dto.setStartTime(flinkProcessPO.getStartTime());
                    dto.setTaskConfig(configs);
                    dto.setLogMessage(logMessages);
                    dto.setYarnAppId(flinkProcessPO.getYarnAppId());
                    dto.setType(TaskSubmitTypeEnum.getDescription(flinkProcessPO.getSubmitType()));
                    dto.setResult(flinkProcessPO.getSubmitResult());
                    rsDtos.add(dto);
                }catch (Exception e){
                }
            }
        }
        //查询列表
        pageResultDTO.setList(rsDtos);
        //查询总数
        pageResultDTO.setCount(flinkProcessDao.queryCount(processPageQueryDTO));
        pageResultDTO.setCurrentPage(processPageQueryDTO.getPageNum());
        return pageResultDTO;
    }

    private FlinkTaskDetailDTO getFlinkTaskDetailDto(Integer taskId) {
        TaskPO taskPO = taskDao.getById(taskId);
        if(taskPO == null || taskPO.getProcessId() == null){
            return null;
        }
        FlinkProcessPO flinkProcessPO = flinkProcessDao.getById(taskPO.getProcessId());
        if(flinkProcessPO == null || StringUtils.isBlank(flinkProcessPO.getTaskDetail()) || StringUtils.isBlank(flinkProcessPO.getYarnAppId())){
            return null;
        }
        return JSONObject.parseObject(flinkProcessPO.getTaskDetail(), FlinkTaskDetailDTO.class);
    }

    /**
     * 转换文件大小
     *
     * @param fileS
     * @return
     */
    public String formetFileSize(long fileS) {
        DecimalFormat df = new DecimalFormat("#.00");
        String fileSizeString = "";
        if (fileS < 1024) {
            fileSizeString = df.format((double) fileS) + "B";
        } else if (fileS < 1048576) {
            fileSizeString = df.format((double) fileS / 1024) + "KB";
        } else if (fileS < 1073741824) {
            fileSizeString = df.format((double) fileS / 1048576) + "MB";
        } else {
            fileSizeString = df.format((double) fileS / 1073741824) + "GB";
        }
        return fileSizeString;
    }
}