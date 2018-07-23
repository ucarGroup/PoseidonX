package com.ucar.streamsuite.user.business.impl;

import com.ucar.streamsuite.dao.mysql.UserDao;
import com.ucar.streamsuite.user.business.LoginBusiness;
import com.ucar.streamsuite.user.po.UserPO;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * Description: 登录实现类
 * Created on 2018/1/30 上午9:18
 *
 */

@Service
public class LoginBusinessImpl implements LoginBusiness {

    @Resource
    private UserDao userDao;

    @Override
    public boolean login(String userName, String password) {
        UserPO userPO = userDao.getUserByName(userName);
        if(userPO != null && userPO.getUserName().equals(StringUtils.trimToEmpty(userName)) && userPO.getPassword().equals(StringUtils.trimToEmpty(password))){
            return true;
        }
        return false;
    }
}
