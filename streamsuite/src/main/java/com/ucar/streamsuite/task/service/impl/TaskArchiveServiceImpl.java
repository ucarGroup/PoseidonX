package com.ucar.streamsuite.task.service.impl;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.filter.WebContextHolder;
import com.ucar.streamsuite.common.util.StreamUtil;
import com.ucar.streamsuite.dao.mysql.TaskArchiveDao;
import com.ucar.streamsuite.dao.mysql.TaskArchiveVersionDao;
import com.ucar.streamsuite.task.dto.TaskArchiveDTO;
import com.ucar.streamsuite.task.dto.TaskArchivePageQueryDTO;
import com.ucar.streamsuite.task.po.TaskArchiveVersionPO;
import com.ucar.streamsuite.task.po.TaskArchivePO;
import com.ucar.streamsuite.task.service.TaskArchiveService;
import com.ucar.streamsuite.user.po.UserGroupPO;
import com.ucar.streamsuite.user.po.UserPO;
import com.ucar.streamsuite.user.service.UserGroupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;

/**
 * Description: 任务文件 service 实现类
 * Created on 2018/2/9 上午10:07
 *
 */
@Service
public class TaskArchiveServiceImpl implements TaskArchiveService {

    @Resource
    private TaskArchiveDao taskArchiveDao;

    @Resource
    private TaskArchiveVersionDao taskArchiveVersionDao;

    @Autowired
    private UserGroupService userGroupService;


    public PageResultDTO pageQuery(TaskArchivePageQueryDTO taskArchivePageQueryDTO) {

        PageResultDTO pageResultDTO = new PageResultDTO();
        PageQueryDTO pageQueryDTO = StreamUtil.dealPageQuery((PageQueryDTO)taskArchivePageQueryDTO);

        List<TaskArchivePO> taskArchivePOList  = taskArchiveDao.pageQuery(pageQueryDTO);
        List<TaskArchiveDTO> taskArchiveDTOList = new ArrayList<TaskArchiveDTO>();

        for(TaskArchivePO taskArchivePO : taskArchivePOList){

            TaskArchiveDTO taskArchiveDTO = new TaskArchiveDTO();
            taskArchiveDTO.setId(taskArchivePO.getId());
            taskArchiveDTO.setTaskArchiveName(taskArchivePO.getTaskArchiveName());
            taskArchiveDTO.setTaskArchiveRemark(taskArchivePO.getTaskArchiveRemark());
            taskArchiveDTO.setUserGroupId(taskArchivePO.getUserGroupId());

            if(taskArchivePO.getUserGroupId() != null) {
                UserGroupPO userGroupPO = userGroupService.getUserGroupById(taskArchivePO.getUserGroupId());
                if(userGroupPO != null ){
                    taskArchiveDTO.setUserGroupName(userGroupPO.getName());
                }
            }
            taskArchiveDTO.setCreateUser(taskArchivePO.getCreateUser());
            taskArchiveDTO.setCreateTime(taskArchivePO.getCreateTime());
            taskArchiveDTO.setTaskArchiveCount(taskArchiveVersionDao.queryCountByArchiveId(taskArchivePO.getId()));
            taskArchiveDTOList.add(taskArchiveDTO);
        }


        pageResultDTO.setList(taskArchiveDTOList);
        pageResultDTO.setCount(taskArchiveDao.queryCount());
        pageResultDTO.setCurrentPage(pageQueryDTO.getPageNum());

        return pageResultDTO;
    }

    public TaskArchivePO getArchiveById(Integer archiveId) {
        return taskArchiveDao.getTaskArchiveById(archiveId);
    }

    public boolean insertArchive(TaskArchivePO taskArchivePO) {
        TaskArchivePageQueryDTO taskQueryDTO = new TaskArchivePageQueryDTO();
        taskQueryDTO.setTaskArchiveName(taskArchivePO.getTaskArchiveName());
        int result = taskArchiveDao.queryCount(taskQueryDTO);
        if(result>0){
            throw new RuntimeException("该任务文件名称已存在");
        }
        UserPO userPO = WebContextHolder.getLoginUser();
        taskArchivePO.setCreateUser(userPO.getUserName());
        taskArchivePO.setCreateTime(new Date());
        return taskArchiveDao.insertNewTaskArchive(taskArchivePO);
    }

    public void updateArchive(TaskArchivePO taskArchivePO) {
        taskArchivePO.setModifyTime(new Date());
        taskArchiveDao.updateTaskArchive(taskArchivePO);
    }

    public boolean insertArchiveVersion(TaskArchiveVersionPO taskArchiveItemPO) {
        UserPO userPO = WebContextHolder.getLoginUser();

        taskArchiveItemPO.setCreateUser(userPO.getUserName());
        taskArchiveItemPO.setCreateTime(new Date());
        return taskArchiveVersionDao.insertNewTaskArchiveVersion(taskArchiveItemPO);
    }

    @Override
    public List<TaskArchivePO> getArchiveByUser(String userName) {
        Map<String,String> params = new HashMap<String,String>();
        params.put("userName",userName);
        return taskArchiveDao.getArchiveByUser(params);
    }

    public List<TaskArchiveVersionPO> getTaskArchiveVersionByArchiveId(Integer archiveId) {
        return taskArchiveVersionDao.getTaskArchiveVersionByArchiveId(archiveId);
    }

    @Override
    public TaskArchiveVersionPO getTaskArchiveVersionById(Integer id) {
        return taskArchiveVersionDao.getTaskArchiveVersionById(id);
    }

}
