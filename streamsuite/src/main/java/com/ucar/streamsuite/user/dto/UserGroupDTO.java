package com.ucar.streamsuite.user.dto;

import java.util.Date;

/**
 * Description:用户组信息的dto
 * Created on 2018/1/31 上午9:36
 *
 */
public class UserGroupDTO {

    private static final long serialVersionUID = -5729044331250315760L;

    private Integer id;
    private String name;
    private String members;
    private Date createTime;
    private Date modifyTime;

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

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }
}
