package com.ucar.streamsuite.user.web;

import com.ucar.streamsuite.common.dto.OperResultDTO;
import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.util.IPUtil;
import com.ucar.streamsuite.user.po.UserLoginHistoryPO;
import com.ucar.streamsuite.user.po.UserPO;
import com.ucar.streamsuite.user.service.UserService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Date;

/**
 * Description: 登录控制器
 * Created on 2018/1/18 下午4:32
 *
 */
@Controller
@RequestMapping("/user")
public class LoginController {

    @Autowired
    private UserService userService;

    @ResponseBody
    @RequestMapping(value = "/loginhistorylist", method = RequestMethod.POST)
    public PageResultDTO list(HttpServletRequest request, HttpServletResponse response) {

        Integer pageNum = Integer.valueOf(request.getParameter("pageNum"));
        Integer pageSize = Integer.valueOf(request.getParameter("pageSize"));

        PageQueryDTO pageQueryDTO = new PageQueryDTO(pageNum,pageSize);
        return  userService.pageQueryUserLoginHistory(pageQueryDTO);

    }

    /**
     * 返回cookie信息,用于调试用途,后期删除
     *
     * @param request
     * @param response
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/cookie")
    public String cookie(HttpServletRequest request, HttpServletResponse response) throws IllegalAccessException, InstantiationException, ClassNotFoundException {

        String result = "none";
        Cookie[] cookies = request.getCookies();//这样便可以获取一个cookie数组
        if (null == cookies) {
            result = "没有cookie=========";
        } else {
            result = "";
            for (Cookie cookie : cookies) {
                result += "name:" + cookie.getName() + ",value:" + cookie.getValue() + "</br>";
            }
        }
        return result;
    }

    @ResponseBody
    @RequestMapping(value = "/clearcookie")
    public String clearCookie(HttpServletResponse response) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        userService.clearUserCookie(response);
        return "true";
    }

    /**
     * 用户登录
     *
     * @param request
     * @param response
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/login", method = RequestMethod.POST)
    public OperResultDTO doLoginPost(HttpServletRequest request, HttpServletResponse response, String username, String password) throws Exception {
        UserPO userPO = userService.login(username, password);
        boolean loginResult = false;

        if (userPO != null) {
            loginResult = true;
        }

        OperResultDTO loginResultDTO = new OperResultDTO();
        loginResultDTO.setResult(loginResult);

        //登录成功
        if (loginResult) {
            loginResultDTO.setMsg(username);

            userPO.setUserName(username);
            //添加用户信息到cookie
            userService.setCookie(response, userPO);

            //添加用户登录历史记录
            UserLoginHistoryPO userLoginHistoryPO = new UserLoginHistoryPO();
            userLoginHistoryPO.setUserName(username);

            String userRole = "普通用户";
            if(userPO.getUserRole() == 0){
                userRole = "超级管理员";
            }
            userLoginHistoryPO.setUserRole(userRole);
            userLoginHistoryPO.setLoginTime(new Date());
            userLoginHistoryPO.setLoginIp(IPUtil.getIpAddr(request));
            userService.insertUserLoginHistory(userLoginHistoryPO);
        }
        return loginResultDTO;
    }

}