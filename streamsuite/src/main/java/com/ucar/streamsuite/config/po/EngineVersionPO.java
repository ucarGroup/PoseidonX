package com.ucar.streamsuite.config.po;

import com.ucar.streamsuite.common.po.BaseTimeLineObject;

import java.io.Serializable;

/**
 * Description:
 * Created on 2018/2/2 下午6:19
 *
 */
public class EngineVersionPO extends BaseTimeLineObject implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;


    /**
     * id（自增id）
     */
    private Integer id;

    private String versionName;
    private String versionType;
    private String versionRemark;
    private String versionUrl;
    private Integer status;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getVersionName() {
        return versionName;
    }

    public void setVersionName(String versionName) {
        this.versionName = versionName;
    }

    public String getVersionType() {
        return versionType;
    }

    public void setVersionType(String versionType) {
        this.versionType = versionType;
    }

    public String getVersionRemark() {
        return versionRemark;
    }

    public void setVersionRemark(String versionRemark) {
        this.versionRemark = versionRemark;
    }

    public String getVersionUrl() {
        return versionUrl;
    }

    public void setVersionUrl(String versionUrl) {
        this.versionUrl = versionUrl;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
