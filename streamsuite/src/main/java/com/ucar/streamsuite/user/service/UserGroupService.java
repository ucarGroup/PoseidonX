package com.ucar.streamsuite.user.service;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.user.po.UserGroupPO;

import java.util.List;

/**
 * Description: 用户组 服务类接口
 * Created on 2018/2/1 上午9:00
 *
 */
public interface UserGroupService {
    PageResultDTO pageQuery(PageQueryDTO pageQueryDTO);

    UserGroupPO getUserGroupById(Integer userGroupId);

    boolean insertUserGroup(UserGroupPO userGroupPO);

    void updateUserGroup(UserGroupPO userGroupPO);

    List<UserGroupPO> queryAll();

}
