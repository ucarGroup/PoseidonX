package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.task.dto.TaskPageQueryDTO;
import com.ucar.streamsuite.task.po.TaskPO;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Description: stream_task 表数据访问类
 * Created on 2018/1/18 下午4:33
 *
 */
@Repository(value="taskDao")
public interface TaskDao {

    /**
     * 根据name获取信息
     *
     * @param taskName
     * @return
     */
    TaskPO getByName(String taskName);

    /**
     * 根据id获取信息
     *
     * @param id
     * @return
     */
    TaskPO getById(int id);

    /**
     * 保存
     *
     * @param taskPO
     * @return
     */
    void insert(TaskPO taskPO);

    /**
     * 更新
     *
     * @param taskPO
     * @return
     */
    void updateInfo(TaskPO taskPO);

    /**
     * 审核任务
     *
     * @param taskPO
     * @return
     */
    void update4Audit(TaskPO taskPO);

    /**
     * 分页查询
     * @param pageQueryDTO
     * @return
     */
    List<TaskPO> pageQuery(TaskPageQueryDTO pageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount(TaskPageQueryDTO pageQueryDTO);

    /**
     * 删除任务
     *
     * @param taskId
     * @return
     */
    void delete(Integer taskId);

    /**
     * 更新processID和时间和状态
     *
     * @param taskPO
     * @return
     */
    void update4Start(TaskPO taskPO);

    /**
     * 更新状态和时间
     *
     * @param taskPO
     * @return
     */
    void update4Stop(TaskPO taskPO);

    /**
     * 更新状态和异常信息
     *
     * @param taskPO
     * @return
     */
    void update4ERROR(TaskPO taskPO);

    /**
     * 查所有的任务
     *
     * @return
     */
    List<TaskPO> listAll(Integer engineType);

}
