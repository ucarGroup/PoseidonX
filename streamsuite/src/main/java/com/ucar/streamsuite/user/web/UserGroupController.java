package com.ucar.streamsuite.user.web;

import com.ucar.streamsuite.common.dto.OperResultDTO;
import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.user.dto.UserGroupDTO;
import com.ucar.streamsuite.user.po.UserGroupPO;
import com.ucar.streamsuite.user.service.UserGroupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;

/**
 * Description: 用户组控制器
 * Created on 2018/2/1 上午9:01
 *

 */
@Controller
@RequestMapping("/usergroup")
public class UserGroupController {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserGroupController.class);

    @Autowired
    private UserGroupService userGroupService;


    @ResponseBody
    @RequestMapping(value = "/list", method = RequestMethod.POST)
    public PageResultDTO list(HttpServletRequest request, HttpServletResponse response) {

        Integer pageNum = Integer.valueOf(request.getParameter("pageNum"));
        Integer pageSize = Integer.valueOf(request.getParameter("pageSize"));

        PageQueryDTO pageQueryDTO = new PageQueryDTO(pageNum,pageSize);
        return  userGroupService.pageQuery(pageQueryDTO);
    }

    @ResponseBody
    @RequestMapping(value = "/listAll", method = RequestMethod.POST)
    public List<UserGroupDTO> listAll(HttpServletRequest request, HttpServletResponse response) {

        List<UserGroupPO> userGroupPOList = userGroupService.queryAll();
        List<UserGroupDTO> userGroupDTOList = new ArrayList<UserGroupDTO>();

        for(UserGroupPO userGroupPO : userGroupPOList){
            UserGroupDTO userGroupDTO = new UserGroupDTO();
            userGroupDTO.setId(userGroupPO.getId());
            userGroupDTO.setName(userGroupPO.getName());
            userGroupDTOList.add(userGroupDTO);
        }

        return userGroupDTOList;
    }

    @ResponseBody
    @RequestMapping(value = "/queryById", method = RequestMethod.POST)
    public UserGroupDTO queryById(HttpServletRequest request, HttpServletResponse response) {

        Integer userGroupId = Integer.valueOf(request.getParameter("id"));

        UserGroupPO userGroupPO = userGroupService.getUserGroupById(userGroupId);

        UserGroupDTO userGroupDTO = new UserGroupDTO();
        userGroupDTO.setId(userGroupPO.getId());
        userGroupDTO.setName(userGroupPO.getName());
        userGroupDTO.setMembers(userGroupPO.getMembers());
        userGroupDTO.setCreateTime(userGroupPO.getCreateTime());
        userGroupDTO.setModifyTime(userGroupPO.getModifyTime());

        return userGroupDTO;
    }


    @ResponseBody
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public OperResultDTO save(HttpServletRequest request, HttpServletResponse response) {

        String id = request.getParameter("id");
        String name = request.getParameter("name");
        String members = request.getParameter("members");

        UserGroupPO userGroupPO = new UserGroupPO();
        userGroupPO.setName(name);
        userGroupPO.setMembers(members);

        OperResultDTO operResultDTO = new OperResultDTO();
        operResultDTO.setResult(true);

        if (operResultDTO.isResult()) {
            //如果id为-1 则表示为新用户组插入,否则为更新已经存在的用户
            if (id == null || id.equals("-1")) {
                boolean result = userGroupService.insertUserGroup(userGroupPO);
                operResultDTO.setResult(result);

                if (!operResultDTO.isResult()) {
                    operResultDTO.setErrMsg("请检查用户组是否已经存在!");
                }
            } else {
                userGroupPO.setId(Integer.valueOf(id));
                userGroupService.updateUserGroup(userGroupPO);
            }
        }



        return operResultDTO;
    }


}
