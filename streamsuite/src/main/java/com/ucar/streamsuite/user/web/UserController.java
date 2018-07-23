package com.ucar.streamsuite.user.web;

import com.ucar.streamsuite.common.dto.OperResultDTO;
import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.util.CheckUtil;
import com.ucar.streamsuite.user.dto.UserDTO;
import com.ucar.streamsuite.user.po.UserPO;
import com.ucar.streamsuite.user.service.UserService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Description: 用户控制器
 * Created on 2018/1/31 上午10:19
 *
 */
@Controller
@RequestMapping("/user")
public class UserController {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserController.class);


    @Autowired
    private UserService userService;

    @ResponseBody
    @RequestMapping(value = "/list", method = RequestMethod.POST)
    public PageResultDTO list(HttpServletRequest request, HttpServletResponse response) {

        Integer pageNum = Integer.valueOf(request.getParameter("pageNum"));
        Integer pageSize = Integer.valueOf(request.getParameter("pageSize"));

        PageQueryDTO pageQueryDTO = new PageQueryDTO(pageNum,pageSize);
        return  userService.pageQuery(pageQueryDTO);

    }

    @ResponseBody
    @RequestMapping(value = "/queryById", method = RequestMethod.POST)
    public UserDTO queryById(HttpServletRequest request, HttpServletResponse response) {

        Integer userId = Integer.valueOf(request.getParameter("id"));
        UserPO userPO = userService.getUserById(userId);

        UserDTO userDTO = new UserDTO();
        userDTO.setId(userPO.getId());
        userDTO.setUserName(userPO.getUserName());
        userDTO.setPassword(userPO.getPassword());
        userDTO.setMobile(userPO.getMobile());
        userDTO.setUserRole(String.valueOf(userPO.getUserRole()));
        userDTO.setUserStatus(String.valueOf(userPO.getUserStatus()));

        return userDTO;
    }


    @ResponseBody
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public OperResultDTO save(HttpServletRequest request, HttpServletResponse response) {

        String userId = request.getParameter("userId");
        String userName = request.getParameter("userName");
        String mobile = request.getParameter("mobile");
        String userRole = request.getParameter("userRole");
        String userStatus = request.getParameter("userStatus");
        String password = request.getParameter("password");

        UserPO userPO = new UserPO();
        userPO.setUserName(userName);
        userPO.setMobile(mobile);
        userPO.setUserRole(Integer.valueOf(userRole));
        userPO.setUserStatus(Integer.valueOf(userStatus));
        userPO.setPassword(password);

        OperResultDTO operResultDTO = new OperResultDTO();
        operResultDTO.setResult(true);

        //校验用户邮箱
        if (!CheckUtil.checkEmail(userName)) {
            operResultDTO.setResult(false);
            operResultDTO.setErrMsg("请检查用户邮箱录入是否有误!");

        }

        //检查用户手机号
        if (StringUtils.isNotBlank(mobile) && !CheckUtil.checkMobile(mobile)){
            operResultDTO.setResult(false);
            operResultDTO.setErrMsg("请检查用户手机号录入是否有误!");
        }


        if (operResultDTO.isResult()) {
            //如果userId为-1 则表示为新用户插入,否则为更新已经存在的用户
            if (userId == null || userId.equals("-1")) {
                boolean result = userService.insertUser(userPO);
                operResultDTO.setResult(result);

                if (!operResultDTO.isResult()) {
                    operResultDTO.setErrMsg("请检查用户是否已经存在!");
                }
            } else {
                userPO.setId(Integer.valueOf(userId));
                userService.updateUser(userPO);
            }
        }



        return operResultDTO;
    }

}
