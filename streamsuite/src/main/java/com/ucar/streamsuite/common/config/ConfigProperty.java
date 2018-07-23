package com.ucar.streamsuite.common.config;


import com.ucar.streamsuite.common.constant.ConfigKeyEnum;
import com.ucar.streamsuite.common.util.PropertiesReader;
import com.ucar.streamsuite.config.po.ConfigPO;
import com.ucar.streamsuite.config.service.ConfigService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

/**
 * Description: 配置文件统一读取客户端
 * 用于读取  streamsuite.properties 等配置文件信息
 * Created on 2018/1/30 上午9:24
 *
 */
@Component
public class ConfigProperty {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigProperty.class);

    @Autowired
    private ConfigService configService;

    private static ConfigProperty configProperty;

    //初始化静态参数
    @PostConstruct
    public void init() {
        configProperty = this;
        configProperty.configService = this.configService;
    }

    /**
     * 基本配置文件名称
     */
    private static final String CONFIG_FILE_NAME = "streamsuite";

    public static final String USER_LOGIN_CLASS = "user.login_class";

    public static final String MONITER_ALARM_CLASS = "moniter.alarm_class";

    public static final String ENVIRONMENT = "environment";

    /**
     * 读取properties配置文件的信息
     *
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        String result = null;
        try {
            Properties config = PropertiesReader.getProperties(CONFIG_FILE_NAME);
            result = config.getProperty(key);
        } catch (Exception e) {
            LOGGER.error("ConfigProperty#getProperty error:fileName:[" + CONFIG_FILE_NAME + "],key:[" + key + "]");
        }
        return result;
    }

    /**
     * 获取数据库中保存的config信息
     *
     * @param configKeyEnum
     * @return
     */
    public static String getConfigValue(ConfigKeyEnum configKeyEnum) {

        if(configProperty != null) {
            ConfigPO configPO = configProperty.configService.getConfigByEnum(configKeyEnum);
            if (configPO != null) {
                return StringUtils.trimToEmpty(configPO.getConfigValue());
            }
        }
        return "";
    }
}
