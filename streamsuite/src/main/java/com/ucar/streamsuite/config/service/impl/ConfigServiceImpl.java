package com.ucar.streamsuite.config.service.impl;

import com.ucar.streamsuite.common.constant.ConfigKeyEnum;
import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.util.StreamUtil;
import com.ucar.streamsuite.config.po.ConfigPO;
import com.ucar.streamsuite.config.service.ConfigService;
import com.ucar.streamsuite.dao.mysql.ConfigDao;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;

/**
 * Description: 配置管理模块服务类实现
 * Created on 2018/2/1 上午9:54
 *
 *
 */
@Service
public class ConfigServiceImpl  implements ConfigService{

    @Resource
    private ConfigDao configDao;

    @Override
    public PageResultDTO pageQuery(PageQueryDTO pageQueryDTO) {
        PageResultDTO pageResultDTO = new PageResultDTO();

        pageQueryDTO = StreamUtil.dealPageQuery(pageQueryDTO);

        //查询列表
        pageResultDTO.setList(configDao.pageQuery(pageQueryDTO));

        //查询总数
        pageResultDTO.setCount(configDao.queryCount());

        pageResultDTO.setCurrentPage(pageQueryDTO.getPageNum());

        return pageResultDTO;
    }

    @Override
    public ConfigPO getConfigById(Integer configId) {
        return configDao.getConfigById(configId);
    }

    @Override
    public ConfigPO getConfigByName(String configName) {
        return configDao.getConfigByName(configName);
    }


    @Override
    public boolean insertConfig(ConfigPO configPO) {
        //判断这个配置是否存在,如果存在就插入失败
        if(configDao.getConfigByName(configPO.getConfigName()) == null) {
            configPO.setCreateTime(new Date());
            configDao.insertNewConfig(configPO);
            return true;
        }
        else{
            return false;
        }
    }

    @Override
    public void updateConfig(ConfigPO configPO) {
        configPO.setModifyTime(new Date());
        configDao.updateConfig(configPO);
    }

    @Override
    public ConfigPO getConfigByEnum(ConfigKeyEnum configKeyEnum) {

        String key = configKeyEnum.name();

        return this.getConfigByName(key);

    }
}
