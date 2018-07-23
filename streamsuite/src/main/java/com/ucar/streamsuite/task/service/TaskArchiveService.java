package com.ucar.streamsuite.task.service;

import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.task.dto.TaskArchivePageQueryDTO;
import com.ucar.streamsuite.task.po.TaskArchiveVersionPO;
import com.ucar.streamsuite.task.po.TaskArchivePO;

import java.util.List;


/**
 * Description: 任务文件 service
 * Created on 2018/2/2 下午6:18
 *
 */
public interface TaskArchiveService {

    PageResultDTO pageQuery(TaskArchivePageQueryDTO taskArchivePageQueryDTO);

    TaskArchivePO getArchiveById(Integer archiveId);

    boolean insertArchive(TaskArchivePO taskArchivePO);

    void updateArchive(TaskArchivePO taskArchivePO);

    boolean insertArchiveVersion(TaskArchiveVersionPO taskArchiveVersionPO);

    List<TaskArchivePO> getArchiveByUser(String userName);

    List<TaskArchiveVersionPO> getTaskArchiveVersionByArchiveId(Integer archiveId);

    TaskArchiveVersionPO getTaskArchiveVersionById(Integer id);

}
