package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.task.po.TaskArchiveVersionPO;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Description:任务文件明细模块DAO
 * Created on 2018/2/1 上午9:52
 *
 */
@Repository(value="taskArchiveVersionDao")
public interface TaskArchiveVersionDao {

    /**
     * 通过id获取项目明细信息
     * @param id
     * @return
     */
    TaskArchiveVersionPO getTaskArchiveVersionById(int id);

    /**
     * 通过项目id获取项目明细信息
     * @param archiveId
     * @return
     */
    List<TaskArchiveVersionPO> getTaskArchiveVersionByArchiveId(int archiveId);

    /**
     * 插入新的项目明细信息
     * @param taskArchiveVersionPO
     * @return
     */
    boolean insertNewTaskArchiveVersion(TaskArchiveVersionPO taskArchiveVersionPO);


    /**
     * 通过文件id获取文件明细数量
     * @param archiveId
     * @return
     */
    Integer queryCountByArchiveId(Integer archiveId);

    /**
     * 通过引擎名称拿到引擎地址
     * @param archiveName
     * @return
     */
    String getCqlEngineUrl(String archiveName);

}
