package com.ucar.streamsuite.user.service.impl;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.util.StreamUtil;
import com.ucar.streamsuite.dao.mysql.UserGroupDao;
import com.ucar.streamsuite.user.po.UserGroupPO;
import com.ucar.streamsuite.user.service.UserGroupService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;

/**
 * Description: 用户组 服务类接口
 * Created on 2018/2/1 上午9:00
 *
 */
@Service
public class UserGroupServiceImpl implements UserGroupService {

    @Resource
    private UserGroupDao userGroupDao;

    @Override
    public PageResultDTO pageQuery(PageQueryDTO pageQueryDTO) {
        PageResultDTO pageResultDTO = new PageResultDTO();

        pageQueryDTO = StreamUtil.dealPageQuery(pageQueryDTO);

        //查询列表
        pageResultDTO.setList(userGroupDao.pageQuery(pageQueryDTO));

        //查询总数
        pageResultDTO.setCount(userGroupDao.queryCount());

        pageResultDTO.setCurrentPage(pageQueryDTO.getPageNum());

        return pageResultDTO;
    }

    @Override
    public UserGroupPO getUserGroupById(Integer userGroupId) {
        return userGroupDao.getUserGroupById(userGroupId);
    }

    @Override
    public boolean insertUserGroup(UserGroupPO userGroupPO) {
        //判断这个用户组是否存在,如果存在就插入失败
        if(userGroupDao.getUserGroupByName(userGroupPO.getName()) == null) {
            userGroupPO.setCreateTime(new Date());
            userGroupDao.insertNewUserGroup(userGroupPO);
            return true;
        }
        else{
            return false;
        }
    }

    @Override
    public void updateUserGroup(UserGroupPO userGroupPO) {
        userGroupPO.setModifyTime(new Date());
        userGroupDao.updateUserGroup(userGroupPO);
    }

    @Override
    public List<UserGroupPO> queryAll() {
        return userGroupDao.queryAll();
    }
}
