package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.user.po.UserGroupPO;

import java.util.List;

/**
 * Description: 用户组信息dao
 *              用户组用于权限管理
 * Created on 2018/2/1 上午8:56
 *
 */
public interface UserGroupDao {

    /**
     * 通过id获取用户组信息
     * @param id
     * @return
     */
    UserGroupPO getUserGroupById(int id);

    /**
     * 通过组名查询用户组
     * @param name
     * @return
     */
    UserGroupPO getUserGroupByName(String name);

    /**
     * 根据任务ID查询用户组信息
     * @param taskId
     * @return
     */
    UserGroupPO getUserGroupByTaskId(int taskId);

    /**
     * 插入新的用户组
     * @param userGroupPO
     * @return
     */
    boolean insertNewUserGroup(UserGroupPO userGroupPO);

    /**
     * 更新用户组
     * @param userGroupPO
     * @return
     */
    boolean updateUserGroup(UserGroupPO userGroupPO);

    /**
     * 分页查询
     * @param pageQueryDTO
     * @return
     */
    List<UserGroupPO> pageQuery(PageQueryDTO pageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount();


    /**
     * 查询用户组全部
     * @return
     */
    List<UserGroupPO> queryAll();
}
