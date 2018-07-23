package com.ucar.streamsuite.task.service.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ucar.streamsuite.common.constant.*;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.util.StreamUtil;
import com.ucar.streamsuite.common.filter.WebContextHolder;
import com.ucar.streamsuite.cql.po.CqlPO;
import com.ucar.streamsuite.dao.mysql.*;
import com.ucar.streamsuite.engine.business.EngineBusiness;

import com.ucar.streamsuite.engine.po.JstormProcessPO;
import com.ucar.streamsuite.task.dto.TaskDTO;
import com.ucar.streamsuite.task.dto.TaskPageQueryDTO;
import com.ucar.streamsuite.task.dto.TaskStartTimeLineDTO;
import com.ucar.streamsuite.task.po.TaskArchivePO;
import com.ucar.streamsuite.task.po.TaskArchiveVersionPO;
import com.ucar.streamsuite.task.po.TaskPO;
import com.ucar.streamsuite.task.service.TaskService;
import com.ucar.streamsuite.user.po.UserPO;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Description: 任务服务类
 * Created on 2018/1/18 下午4:33
 *
 */
@Service
public class TaskServiceImpl implements TaskService {

    @Resource
    private TaskDao taskDao;

    @Resource
    private CqlDao cqlDao;

    @Resource
    private EngineVersionDao engineVersionDao;

    @Resource
    private TaskArchiveDao taskArchiveDao;

    @Resource
    private TaskArchiveVersionDao taskArchiveVersionDao;

    @Override
    public void saveTask(TaskDTO taskDTO) throws Exception{
        UserPO userPO = WebContextHolder.getLoginUser();
        if(userPO == null){
            throw new Exception("保存任务失败，用户会话超时，请重新登录再操作！");
        }
        if(StringUtils.isBlank(taskDTO.getTaskName())){
            throw new Exception("任务名不能为空！");
        }
        if(taskDao.getByName(taskDTO.getTaskName()) != null){
            throw new Exception("已存在相同任务名称的任务！");
        }

        TaskPO taskPO = new TaskPO();

        if(taskDTO.getEngineType() == null){
            throw new Exception("请选择引擎类型！");
        }
        taskPO.setEngineType(taskDTO.getEngineType());
        taskPO.setTaskConfig(EngineBusiness.validateAndBuildTaskConfig(taskDTO,taskDTO.getEngineType()));

        if(taskDTO.getIsCql() == YesOrNoEnum.NO.getValue()){
            TaskArchivePO taskArchivePO = taskArchiveDao.getTaskArchiveById(Integer.valueOf(taskDTO.getArchiveId()));
            taskPO.setArchiveId(Integer.valueOf(taskDTO.getArchiveId()));
            taskPO.setArchiveVersionId(Integer.valueOf(taskDTO.getArchiveVersionId()));
            // 用户组为选择的任务运行文件的用户组的ID
            taskPO.setUserGroupId(taskArchivePO.getUserGroupId());
        }else{
            CqlPO cqlPO = cqlDao.getCqlById(Integer.valueOf(taskDTO.getTaskCqlId()));
            // 用户组为CQL的用户组
            taskPO.setUserGroupId(cqlPO.getUserGroupId());
            taskPO.setArchiveId(null);
            taskPO.setArchiveVersionId(null);
        }
        taskPO.setIsCql(taskDTO.getIsCql());

        taskPO.setTaskStatus(TaskStatusEnum.WAIT.ordinal());
        taskPO.setAuditStatus(AuditStatusEnum.WAIT.getValue());
        taskPO.setCreatorUserName(userPO.getUserName());
        taskPO.setDeleteStatus(CommonStatusEnum.ENABLE.ordinal());
        taskPO.setTaskName(taskDTO.getTaskName());
        taskPO.setCreateTime(new Date());
        taskPO.setRemark(taskDTO.getRemark());
        taskDao.insert(taskPO);
    }

    @Override
    public void updateTask(TaskDTO taskDTO) throws Exception{
        TaskPO updatePO = new TaskPO();
        updatePO.setId(taskDTO.getId());

        TaskPO taskPO = taskDao.getById(taskDTO.getId());
        if(taskPO == null){
            throw new Exception("任务信息不存在或者已经被删除！");
        }
        UserPO userPO = WebContextHolder.getLoginUser();
        if(userPO == null){
            throw new Exception("修改任务时失败，用户会话超时，请重新登录再操作！");
        }
        if(!validateUser(userPO,taskPO.getCreatorUserName())){
            throw new Exception("您没有权限执行此操作，只有管理员和任务创建人可以开始任务！");
        }
        if(taskPO.getTaskStatus()!= TaskStatusEnum.WAIT.ordinal() && taskPO.getTaskStatus()!= TaskStatusEnum.STOP.ordinal()){
            throw new Exception("修改任务时失败，只有未开始和终止运行状态的任务才可以执行修改操作！");
        }

        updatePO.setEngineType(taskPO.getEngineType());
        updatePO.setTaskConfig(EngineBusiness.validateAndBuildTaskConfig(taskDTO,taskPO.getEngineType()));

        if(taskDTO.getIsCql() == YesOrNoEnum.NO.getValue()){
            TaskArchivePO taskArchivePO = taskArchiveDao.getTaskArchiveById(Integer.valueOf(taskDTO.getArchiveId()));
            // 用户组为选择的任务运行文件的用户组的ID
            updatePO.setArchiveId(Integer.valueOf(taskDTO.getArchiveId()));
            updatePO.setArchiveVersionId(Integer.valueOf(taskDTO.getArchiveVersionId()));
            updatePO.setUserGroupId(taskArchivePO.getUserGroupId());
        }else{
            CqlPO cqlPO = cqlDao.getCqlById( Integer.valueOf(taskDTO.getTaskCqlId()));

            updatePO.setUserGroupId(cqlPO.getUserGroupId());
            updatePO.setArchiveId(null);
            updatePO.setArchiveVersionId(null);
        }
        updatePO.setIsCql(taskDTO.getIsCql());
        updatePO.setModifyUserName(userPO.getUserName());
        updatePO.setModifyTime(new Date());
        updatePO.setRemark(taskDTO.getRemark());
        taskDao.updateInfo(updatePO);
    }

    @Override
    public void aduit(Integer taskId, Integer aduitStatus) throws Exception{
        TaskPO taskPO = getTaskById(taskId);
        if(taskPO == null){
            throw new Exception("任务信息不存在，或已经被删除，请刷新列表后重新操作！");
        }
        if(taskPO.getAuditStatus() != AuditStatusEnum.WAIT.getValue()){
            throw new Exception("只有待审核状态的任务才可以进行审核！");
        }
        UserPO userPO = WebContextHolder.getLoginUser();
        if(userPO == null){
            throw new Exception("审核任务时失败，用户会话超时，请重新登录再操作！");
        }
        if(userPO.getUserRole() != UserRoleEnum.SUPER_ADMIN.ordinal()){
            throw new Exception("对不起，您不是管理员，无法执行审核任务操作！");
        }
        TaskPO updatePo = new TaskPO();
        updatePo.setAuditStatus(aduitStatus);
        updatePo.setAuditTime(new Date());
        updatePo.setAuditUserName(userPO.getUserName());
        updatePo.setId(taskId);
        taskDao.update4Audit(updatePo);
    }

    @Override
    public void beginTask(Integer taskId) throws Exception{
        TaskPO taskPO = taskDao.getById(taskId);
        if(taskPO == null){
            throw new Exception("开始任务时发生异常，任务信息不存在或者已经被删除！");
        }

        UserPO userPO = WebContextHolder.getLoginUser();
        if(userPO == null){
            throw new Exception("开始任务时失败，用户会话超时，请重新登录再操作！");
        }
        if(!validateUser(userPO,taskPO.getCreatorUserName())){
            throw new Exception("您没有权限执行此操作，只有管理员和任务创建人可以开始任务！");
        }

        //校验审核状态
        if(taskPO.getAuditStatus() != AuditStatusEnum.PASS.getValue()){
            throw new Exception("开始任务时失败，只有审核通过的任务才可以执行开始操作！");
        }
        //校验任务状态 只能是未开始和终止运行状态的开始执行
        if(taskPO.getTaskStatus()!= TaskStatusEnum.WAIT.ordinal() && taskPO.getTaskStatus()!= TaskStatusEnum.STOP.ordinal()){
            throw new Exception("开始任务时失败，只有未开始和终止运行状态的任务才可以执行开始操作！");
        }
        EngineBusiness.beginTask(taskId,taskPO.getTaskConfig(),taskPO.getEngineType());
    }

    @Override
    public void stopTask(Integer taskId) throws Exception{
        TaskPO taskPO = taskDao.getById(taskId);
        if(taskPO == null){
            throw new Exception("停止任务时发生异常，任务信息不存在或者已经被删除！");
        }

        UserPO userPO = WebContextHolder.getLoginUser();
        if(userPO == null){
            throw new Exception("停止任务时失败，用户会话超时，请重新登录再操作！");
        }
        if(!validateUser(userPO,taskPO.getCreatorUserName())){
            throw new Exception("您没有权限执行此操作，只有管理员和任务创建人可以停止任务！");
        }

        //校验任务状态 运行中的和异常中止的才可以停止
        if(taskPO.getTaskStatus()!= TaskStatusEnum.RUNNING.ordinal() && taskPO.getTaskStatus()!= TaskStatusEnum.ERROR.ordinal()){
            throw new Exception("停止任务时失败，只有运行中的和异常中止状态的任务才可以执行停止操作！");
        }
        EngineBusiness.stopTask(taskId,taskPO.getTaskName(),taskPO.getProcessId(),taskPO.getEngineType());
    }

    @Override
    public PageResultDTO pageQuery(TaskPageQueryDTO pageQueryDTO) {
        UserPO userPO = WebContextHolder.getLoginUser();
        if(userPO == null){
            return new PageResultDTO();
        }
        if(userPO.getUserRole() != UserRoleEnum.SUPER_ADMIN.ordinal()){
            pageQueryDTO.setCurrentUserNameCondition(userPO.getUserName());
        }

        StreamUtil.dealPageQuery(pageQueryDTO);

        PageResultDTO<TaskDTO> pageResultDTO = new PageResultDTO<TaskDTO>();

        List<TaskDTO> taskDTOs= Lists.newArrayList();
        List<TaskPO> taskPOs= taskDao.pageQuery(pageQueryDTO);

        if(CollectionUtils.isNotEmpty(taskPOs)){
            for(TaskPO taskPO:taskPOs){
                TaskDTO taskDTO = convertPoToDto(taskPO,false);
                taskDTOs.add(taskDTO);
            }
        }
        //查询列表
        pageResultDTO.setList(taskDTOs);
        //查询总数
        pageResultDTO.setCount(taskDao.queryCount(pageQueryDTO));
        pageResultDTO.setCurrentPage(pageQueryDTO.getPageNum());
        return pageResultDTO;
    }

    @Override
    public TaskPO getTaskById(Integer taskId) {
        return taskDao.getById(taskId);
    }

    @Override
    public void deleteTask(Integer taskId) throws Exception {
        TaskPO taskPO = getTaskById(taskId);
        if(taskPO == null){
            throw new Exception("任务信息不存在，或已经被删除，请刷新列表后重新操作！");
        }
        UserPO userPO = WebContextHolder.getLoginUser();
        if(userPO == null){
            throw new Exception("删除任务时失败，用户会话超时，请重新登录再操作！");
        }
        if(userPO.getUserRole() != UserRoleEnum.SUPER_ADMIN.ordinal()){
            throw new Exception("对不起，您不是管理员，无法执行删除任务操作！");
        }
        if(taskPO.getTaskStatus()!= TaskStatusEnum.WAIT.ordinal() && taskPO.getTaskStatus()!= TaskStatusEnum.STOP.ordinal()){
            throw new Exception("删除任务时失败，只有未开始和终止运行状态的任务才可以执行删除操作！");
        }
        taskDao.delete(taskId);
    }

    @Override
    public TaskDTO convertPoToDto(TaskPO taskPO,boolean withShow) {
        if(taskPO == null){
            return null;
        }
        TaskDTO taskDTO = new TaskDTO();
        taskDTO.setId(taskPO.getId());
        taskDTO.setTaskName(taskPO.getTaskName());
        taskDTO.setTaskType(taskPO.getTaskType());
        taskDTO.setTaskStatus(taskPO.getTaskStatus());
        taskDTO.setAuditStatus(taskPO.getAuditStatus());
        taskDTO.setAuditTime(taskPO.getAuditTime());
        taskDTO.setCreateTime(taskPO.getCreateTime());
        taskDTO.setTaskStartTime(taskPO.getTaskStartTime());
        taskDTO.setTaskStopTime(taskPO.getTaskStopTime());
        taskDTO.setErrorInfo(taskPO.getErrorInfo());
        taskDTO.setArchiveId(taskPO.getArchiveId()==null?"":taskPO.getArchiveId().toString());
        taskDTO.setArchiveVersionId(taskPO.getArchiveVersionId()==null?"":taskPO.getArchiveVersionId().toString());
        taskDTO.setCreatorUserName(taskPO.getCreatorUserName());
        taskDTO.setRemark(taskPO.getRemark());
        taskDTO.setModifyUserName(taskPO.getModifyUserName());
        taskDTO.setModifyTime(taskPO.getModifyTime());
        taskDTO.setAuditUserName(taskPO.getAuditUserName());
        taskDTO.setIsCql(taskPO.getIsCql());
        taskDTO.setEngineType(taskPO.getEngineType());
        taskDTO.setProcessId(taskPO.getProcessId());
        if(withShow){
            //填充详细信息
            taskDTO.setEngineTypeShow(EngineTypeEnum.getDescription(taskPO.getEngineType()));
            taskDTO.setTaskTypeShow(TaskTypeEnum.getDescription(taskPO.getTaskType()));
            taskDTO.setTaskStatusShow(TaskStatusEnum.getDescription(taskPO.getTaskStatus()));
            taskDTO.setAuditStatusShow(AuditStatusEnum.getDescription(taskPO.getAuditStatus()));
            if(StringUtils.isNotBlank(taskDTO.getArchiveId())){
                TaskArchivePO taskArchivePO = taskArchiveDao.getTaskArchiveById(Integer.valueOf(taskDTO.getArchiveId()));
                if(taskArchivePO != null ){
                    taskDTO.setArchiveShow(taskArchivePO.getTaskArchiveName());
                }
            }
            if(StringUtils.isNotBlank(taskDTO.getArchiveVersionId())){
                TaskArchiveVersionPO taskArchiveVersionPO = taskArchiveVersionDao.getTaskArchiveVersionById(Integer.valueOf(taskDTO.getArchiveVersionId()));
                if(taskArchiveVersionPO != null ){
                    taskDTO.setArchiveVersionShow(taskArchiveVersionPO.getTaskArchiveVersionUrl());
                }
            }
        }

        EngineBusiness.fillTaskConfigToDto(taskDTO,taskPO.getTaskConfig(),withShow,taskPO.getEngineType());
        return taskDTO;
    }

    @Override
    public TaskStartTimeLineDTO getStartTimeLineByTaskId(Integer taskId) {
        TaskPO taskPO = taskDao.getById(taskId);
        if(taskPO == null){
           return new TaskStartTimeLineDTO();
        }
        TaskStartTimeLineDTO taskStartTimeLineDTO = EngineBusiness.getTaskStartTimeLine(taskPO.getTaskName());
        if(taskStartTimeLineDTO==null){
            return new TaskStartTimeLineDTO();
        }
        return taskStartTimeLineDTO;
    }

    /**
     * 校验权限
     * @param userPO
     * @param validateUserName
     * @return
     */
    private boolean validateUser(UserPO userPO, String validateUserName) {
        if(validateUserName == null){
            return false;
        }
        if(userPO.getUserRole() == UserRoleEnum.SUPER_ADMIN.ordinal()){
            return true;
        }
        //校验用户
        if(userPO.getUserName().equals(validateUserName)){
            return true;
        }
        return false;
    }
}
