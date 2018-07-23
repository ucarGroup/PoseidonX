package com.ucar.streamsuite.config.service;

import com.ucar.streamsuite.common.constant.ConfigKeyEnum;
import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.config.po.ConfigPO;

/**
 * Description: 配置管理模块服务类接口
 * Created on 2018/2/1 上午9:54
 *
 *
 */
public interface ConfigService {

    PageResultDTO pageQuery(PageQueryDTO pageQueryDTO);

    ConfigPO getConfigById(Integer configId);

    ConfigPO getConfigByName (String configName);

    boolean insertConfig(ConfigPO configPO);

    void updateConfig(ConfigPO configPO);

    ConfigPO getConfigByEnum (ConfigKeyEnum configKeyEnum);

}
