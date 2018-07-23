package com.ucar.streamsuite.task.service;

import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.task.dto.TaskDTO;
import com.ucar.streamsuite.task.dto.TaskPageQueryDTO;
import com.ucar.streamsuite.task.dto.TaskStartTimeLineDTO;
import com.ucar.streamsuite.task.po.TaskPO;

/**
 * Description: 任务服务类
 * Created on 2018/1/18 下午4:33
 *
 */
public interface TaskService {

     /**
      * 开始任务
      * @param taskId
      * @throws Exception
      */
     void beginTask(Integer taskId) throws Exception;

     /**
      * 停止任务
      * @param taskId
      * @throws Exception
      */
     void stopTask(Integer taskId) throws Exception;

     /**
      * 删除任务
      * @param taskId
      * @throws Exception
      */
     void deleteTask(Integer taskId) throws Exception;

     /**
      * 保存
      * @param taskDTO
      * @throws Exception
      */
     public void saveTask(TaskDTO taskDTO) throws Exception;

     /**
      * 分页查询
      * @param pageQueryDTO
      * @return
      */
     PageResultDTO pageQuery(TaskPageQueryDTO pageQueryDTO);

     /**
      * 根据ID查询
      * @param taskId
      * @return
      */
     public TaskPO getTaskById(Integer taskId);

     /**
      * 审批
      * @param taskId
      * @param aduitStatus
      */
     public void aduit(Integer taskId,Integer aduitStatus) throws Exception;

     /**
      * 将PO转成DTO
      * @param taskPO
      * @return
      */
     public TaskDTO convertPoToDto(TaskPO taskPO,boolean withShow);

     /**
      * 更新
      * @param taskDTO
      * @throws Exception
      */
     public void updateTask(TaskDTO taskDTO) throws Exception;

     /**
      * 获得开始过程的时间线内容。
      * @param taskId
      * @return
      */
     public TaskStartTimeLineDTO getStartTimeLineByTaskId(Integer taskId);

}
