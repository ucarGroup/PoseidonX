package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.task.po.TaskPO;
import com.ucar.streamsuite.user.po.UserPO;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * Description: 用户信息Dao
 * Created on 2018/1/30 上午9:11
 *
 */
@Repository(value="userDao")
public interface UserDao {

    /**
     * 根据id获取用户信息
     *
     * @param id 用户id
     * @return
     */
    UserPO getUserById(int id);

    /**
     * 根据用户名获取用户信息
     *
     * @param userName 用户名
     * @return
     */
    UserPO getUserByName(String userName);

    /**
     * 插入新的用户信息
     *
     * @param user 用户信息
     * @return
     */
    boolean insertNewUser(UserPO user);

    /**
     * 更新用户信息
     *
     * @param user 用户信息
     * @return
     */
    boolean updateUser(UserPO user);


    /**
     * 分页查询
     * @param pageQueryDTO
     * @return
     */
    List<UserPO> pageQuery(PageQueryDTO pageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount();

    /**
     * 查所有任务文件相关人 包括所有管理员
     *
     * @return
     */
    List<UserPO> listTaskRelationUsers(Map<String,Object> map);

}
