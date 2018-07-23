package com.ucar.streamsuite.user.po;

import com.ucar.streamsuite.common.po.BaseTimeLineObject;

import java.io.Serializable;

/**
 * Description: 用户组管理,
 * 用户组用来设置其他功能模块的 权限使用
 * Created on 2018/2/1 上午8:49
 *
 */
public class UserGroupPO extends BaseTimeLineObject implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    /**
     * id（自增id）
     */
    private Integer id;

    /**
     * 用户组名
     */
    private String name;

    /**
     * 用户组成员列表, 用逗号分隔
     */
    private String members;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMembers() {
        return members;
    }

    public void setMembers(String members) {
        this.members = members;
    }
}
