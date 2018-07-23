package com.ucar.streamsuite.common.web;


import com.ucar.streamsuite.common.constant.StreamContant;
import com.ucar.streamsuite.user.po.UserPO;

import javax.servlet.http.HttpServletRequest;

/**
 * Description: 控制器基类
 * Created on 2018/1/18 下午4:32
 *
 */
public class BaseController {

    /**
     * 查询用户session信息
     *
     * @param request
     * @return
     */
    protected UserPO retrieveUser(HttpServletRequest request) {
        return (UserPO) request.getSession().getAttribute(StreamContant.SESSION_USER);
    }

}
