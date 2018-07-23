package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.user.po.UserLoginHistoryPO;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Description: 用户登录历史纪录信息Dao
 * Created on 2018/1/30 上午9:11
 *
 */
@Repository(value="userLoginHistoryDao")
public interface UserLoginHistoryDao {


    /**
     * 插入新的用户登录历史信息
     *
     * @param userLoginHistoryPO 用户登录历史信息
     * @return
     */
    boolean insertNewUserLoginHistory(UserLoginHistoryPO userLoginHistoryPO);



    /**
     * 分页查询
     * @param pageQueryDTO
     * @return
     */
    List<UserLoginHistoryPO> pageQuery(PageQueryDTO pageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount();

}
