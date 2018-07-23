package com.ucar.streamsuite.user.service;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.user.po.UserLoginHistoryPO;
import com.ucar.streamsuite.user.po.UserPO;

import javax.servlet.http.HttpServletResponse;
import java.util.Map;

/**
 * Description: 用户服务类接口
 * Created on 2018/1/31 上午8:48
 *
 */
public interface UserService {

    UserPO login(String username, String password) throws Exception;

    void setCookie(HttpServletResponse response, UserPO userPO);

    boolean insertUser(UserPO userDTO);

    void updateUser(UserPO userDTO);

    PageResultDTO pageQuery(PageQueryDTO pageQueryDTO);

    UserPO getUserById(Integer userId);

    void clearUserCookie(HttpServletResponse response);

    void insertUserLoginHistory(UserLoginHistoryPO userLoginHistoryPO);

    PageResultDTO pageQueryUserLoginHistory(PageQueryDTO pageQueryDTO);

    Map<String,String> listTaskRelationUserTels(Integer taskId);
}
