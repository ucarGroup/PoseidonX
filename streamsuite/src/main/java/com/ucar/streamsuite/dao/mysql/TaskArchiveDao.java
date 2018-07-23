package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.task.dto.TaskArchivePageQueryDTO;
import com.ucar.streamsuite.task.po.TaskArchivePO;

import java.util.List;
import java.util.Map;

/**
 * Description:任务文件模块DAO
 * Created on 2018/2/1 上午9:52
 *
 */
public interface TaskArchiveDao {

    /**
     * 通过id获取任务文件信息
     * @param id
     * @return
     */
    TaskArchivePO getTaskArchiveById(int id);


    /**
     * 插入新的任务文件信息
     * @param projectPO
     * @return
     */
    boolean insertNewTaskArchive(TaskArchivePO projectPO);

    /**
     * 更新任务文件信息
     * @param projectPO
     * @return
     */
    boolean updateTaskArchive(TaskArchivePO projectPO);

    /**
     * 分页查询
     * @param pageQueryDTO
     * @return
     */
    List<TaskArchivePO> pageQuery(PageQueryDTO pageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount();

    /**
     * 通过条件查询数量
     * @param taskArchivePageQueryDTO
     * @return
     */
    Integer queryCount(TaskArchivePageQueryDTO taskArchivePageQueryDTO);

    /**
     * 通过用户查询项目文件
     * @return
     */
    List<TaskArchivePO> getArchiveByUser(Map<String,String> params);
}
