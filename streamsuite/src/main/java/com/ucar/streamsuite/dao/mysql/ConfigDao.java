package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.config.po.ConfigPO;

import java.util.List;

/**
 * Description:配置模块DAO
 * Created on 2018/2/1 上午9:52
 *
 */
public interface ConfigDao {

    /**
     * 通过id获取配置组信息
     * @param id
     * @return
     */
    ConfigPO getConfigById(int id);

    /**
     * 通过组名查询配置信息
     * @param name
     * @return
     */
    ConfigPO getConfigByName(String name);

    /**
     * 插入新的配置信息
     * @param configPO
     * @return
     */
    boolean insertNewConfig(ConfigPO configPO);

    /**
     * 更新配置信息
     * @param configPO
     * @return
     */
    boolean updateConfig(ConfigPO configPO);

    /**
     * 分页查询
     * @param pageQueryDTO
     * @return
     */
    List<ConfigPO> pageQuery(PageQueryDTO pageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount();

}
