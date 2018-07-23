package com.ucar.streamsuite.config.dto;

import java.io.Serializable;
import java.util.Date;

/**
 * Description:
 * Created on 2018/2/1 上午10:05
 *
 */
public class ConfigDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    private Integer id;
    private String configName;
    private String configValue;
    private String configRemark;
    private Date createTime;
    private Date modifyTime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getConfigName() {
        return configName;
    }

    public void setConfigName(String configName) {
        this.configName = configName;
    }

    public String getConfigValue() {
        return configValue;
    }

    public void setConfigValue(String configValue) {
        this.configValue = configValue;
    }

    public String getConfigRemark() {
        return configRemark;
    }

    public void setConfigRemark(String configRemark) {
        this.configRemark = configRemark;
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
