package com.ucar.streamsuite.config.po;

import com.ucar.streamsuite.common.po.BaseTimeLineObject;

import java.io.Serializable;

/**
 * Description: 配置模块信息
 * Created on 2018/2/1 上午9:55
 *
 */
public class ConfigPO extends BaseTimeLineObject implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;
    /**
     * id（自增id）
     */
    private Integer id;

    /**
     * 配置项名字
     */
    private String configName;

    /**
     * 配置项的值
     */
    private String configValue;

    /**
     * 配置项的说明
     */
    private String configRemark;

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
}
