package com.ucar.streamsuite.user.po;

import java.io.Serializable;
import java.util.Date;

/**
 * Description:
 * Created on 2018/3/15 上午10:37
 *
 */
public class UserLoginHistoryPO implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    private Integer id;
    private String userName;
    private String userRole;
    private String loginIp;
    private Date loginTime;

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

    public String getUserRole() {
        return userRole;
    }

    public void setUserRole(String userRole) {
        this.userRole = userRole;
    }

    public String getLoginIp() {
        return loginIp;
    }

    public void setLoginIp(String loginIp) {
        this.loginIp = loginIp;
    }

    public Date getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(Date loginTime) {
        this.loginTime = loginTime;
    }
}
