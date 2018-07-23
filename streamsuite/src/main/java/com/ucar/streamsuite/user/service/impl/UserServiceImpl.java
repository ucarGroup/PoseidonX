package com.ucar.streamsuite.user.service.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.CommonStatusEnum;
import com.ucar.streamsuite.common.constant.StreamContant;
import com.ucar.streamsuite.common.constant.UserRoleEnum;
import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.util.StreamUtil;
import com.ucar.streamsuite.dao.mysql.UserDao;
import com.ucar.streamsuite.dao.mysql.UserGroupDao;
import com.ucar.streamsuite.dao.mysql.UserLoginHistoryDao;
import com.ucar.streamsuite.moniter.service.FlinkReportService;
import com.ucar.streamsuite.user.business.LoginBusiness;
import com.ucar.streamsuite.user.po.UserGroupPO;
import com.ucar.streamsuite.user.po.UserLoginHistoryPO;
import com.ucar.streamsuite.user.po.UserPO;
import com.ucar.streamsuite.user.service.UserService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;

/**
 * Description: 用户服务类实现类
 * Created on 2018/1/30 上午10:59
 *
 */
@Service
public class UserServiceImpl implements UserService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserServiceImpl.class);

    @Resource
    private UserDao userDao;

    @Resource
    private UserLoginHistoryDao userLoginHistoryDao;

    @Resource
    private UserGroupDao userGroupDao;

    @Autowired
    private LoginBusiness loginBusiness;

    /**
     * 执行用户登录
     * 根据用户选择的登录方式进行登录
     * 登录成功后,如果数据库中没有该用户的信息则进行添加
     * @param username
     * @param password
     * @return
     */
    public UserPO login(String username, String password) throws Exception {

        //获取用户登录方式
        String loginClass = ConfigProperty.getProperty(ConfigProperty.USER_LOGIN_CLASS);

        boolean loginResult = loginBusiness.login(username,password);

        //如果登录成功,检查数据库中是否有该用户
        //如果没有该用户则插入该用户信息
        if(loginResult){
            UserPO userPO = userDao.getUserByName(username);

            if(userPO != null && CommonStatusEnum.ENABLE.ordinal() == userPO.getUserStatus()){
                return userPO;
            }
            //如果用户存在,并且没有设置为可用,则登录失败
            else {
                return null;
            }
        }
        //登录失败则返回用户
        else{
            return null;
        }

    }

    /**
     * 将用户信息添加到cookie中
     * 有效期半小时
     * @param response
     * @param userPO
     */
    public void setCookie(HttpServletResponse response, UserPO userPO) {
        Cookie userNameCookie = new Cookie(StreamContant.SESSION_USER, userPO.getUserName());
        userNameCookie.setMaxAge(24 * 60 * 60);// 设置为24小时
        userNameCookie.setPath("/");

        response.addCookie(userNameCookie);

        Cookie userRoleCookie = new Cookie(StreamContant.SESSION_USER_ROLE, String.valueOf(userPO.getUserRole()));
        userRoleCookie.setMaxAge(24 * 60 * 60);// 设置为24小时
        userRoleCookie.setPath("/");

        response.addCookie(userRoleCookie);
    }

    @Override
    public void clearUserCookie(HttpServletResponse response) {
        Cookie userNameCookie = new Cookie(StreamContant.SESSION_USER, null);
        userNameCookie.setMaxAge(0);
        userNameCookie.setPath("/");
        response.addCookie(userNameCookie);
        Cookie userRoleCookie = new Cookie(StreamContant.SESSION_USER_ROLE,null);
        userRoleCookie.setMaxAge(0);
        userRoleCookie.setPath("/");
        response.addCookie(userRoleCookie);
    }

    /**
     * 插入新用户
     * @param userPO
     */
    public boolean insertUser(UserPO userPO) {
        //判断这个用户是否存在,如果存在就插入失败
       if(userDao.getUserByName(userPO.getUserName()) == null) {
           userPO.setCreateTime(new Date());
           userDao.insertNewUser(userPO);
           return true;
       }
       else{
           return false;
       }
    }

    /**
     * 更新已经存在的用户
     * @param userPO
     */
    public void updateUser(UserPO userPO) {
        userPO.setModifyTime(new Date());
        userDao.updateUser(userPO);
    }

    /***
     * 分页查询
     * @param pageQueryDTO
     * @return
     */
    @Override
    public PageResultDTO pageQuery(PageQueryDTO pageQueryDTO) {
        PageResultDTO pageResultDTO = new PageResultDTO();
        pageQueryDTO = StreamUtil.dealPageQuery(pageQueryDTO);
        //查询列表
        pageResultDTO.setList(userDao.pageQuery(pageQueryDTO));
        //查询总数
        pageResultDTO.setCount(userDao.queryCount());
        pageResultDTO.setCurrentPage(pageQueryDTO.getPageNum());
        return pageResultDTO;
    }

    /**
     * 根据id查询用户信息
     * @param userId
     * @return
     */
    @Override
    public UserPO getUserById(Integer userId) {
        return userDao.getUserById(userId);
    }

    @Override
    public void insertUserLoginHistory(UserLoginHistoryPO userLoginHistoryPO) {
        userLoginHistoryDao.insertNewUserLoginHistory(userLoginHistoryPO);
    }

    /***
     * 分页查询 用户登录历史记录
     * @param pageQueryDTO
     * @return
     */
    @Override
    public PageResultDTO pageQueryUserLoginHistory(PageQueryDTO pageQueryDTO) {
        PageResultDTO pageResultDTO = new PageResultDTO();
        pageQueryDTO = StreamUtil.dealPageQuery(pageQueryDTO);
        //查询列表
        pageResultDTO.setList(userLoginHistoryDao.pageQuery(pageQueryDTO));
        //查询总数
        pageResultDTO.setCount(userLoginHistoryDao.queryCount());
        pageResultDTO.setCurrentPage(pageQueryDTO.getPageNum());
        return pageResultDTO;
    }
    /**
     * 给任务的项目相关人员发送告警
     * @param taskId
     */
    @Override
    public  Map<String,String> listTaskRelationUserTels(Integer taskId){
        UserGroupPO userGroupPO = userGroupDao.getUserGroupByTaskId(taskId);
        List<Integer> list = new ArrayList<Integer>();
        if(userGroupPO != null && !StringUtils.isBlank(userGroupPO.getMembers())){
            String[] userIds = userGroupPO.getMembers().split(",");
            for(String userId : userIds){
                try{
                    list.add(Integer.parseInt(userId));
                }catch (Exception e){
                }
            }
        }

        Map<String,Object> map = Maps.newHashMap();
        map.put("ids",list);
        List<UserPO> userPOs = userDao.listTaskRelationUsers(map);
        Map<String,String> userToTel = Maps.newHashMap();
        if(CollectionUtils.isNotEmpty(userPOs)){
            for(UserPO userPO:userPOs){
                userToTel.put(userPO.getUserName(),userPO.getMobile());
            }
        }
        return userToTel;
    }

}
