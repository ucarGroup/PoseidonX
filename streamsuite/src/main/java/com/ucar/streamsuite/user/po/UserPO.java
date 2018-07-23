package com.ucar.streamsuite.user.po;

import com.ucar.streamsuite.common.po.BaseTimeLineObject;

import java.io.Serializable;

/**
 * Description: 用户信息
 * Created on 2018/1/18 下午4:33
 *
 */
public class UserPO extends BaseTimeLineObject implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    /**
     * 用户id（自增id）
     */
    private Integer id;

    /**
     * 用户名(英文，唯一识别用户。必须是邮箱)
     */
    private String userName;

    /**
     * 保留。可用于无LDAP验证使用
     */
    private String password;

    /**
     * 用户手机(用于发报警短信)
     */
    private String mobile;

    /**
     * 用户类型（UserRoleEnum）
     */
    private Integer userRole;

    /**
     * 用户状态（CommonStatusEnum）
     */
    private Integer userStatus;


    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", mobile='" + mobile + '\'' +
                ", userRole=" + userRole +
                "} " + super.toString();
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public Integer getUserRole() {
        return userRole;
    }

    public void setUserRole(Integer userRole) {
        this.userRole = userRole;
    }

    public Integer getUserStatus() {
        return userStatus;
    }

    public void setUserStatus(Integer userStatus) {
        this.userStatus = userStatus;
    }
}
