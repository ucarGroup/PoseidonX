package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.config.po.EngineVersionPO;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Description:引擎版本管理模块DAO
 * Created on 2018/2/1 上午9:52
 *
 */
@Repository(value="engineVersionDao")
public interface EngineVersionDao {

    /**
     * 通过id获取引擎版本管理信息
     * @param id
     * @return
     */
    EngineVersionPO getEngineVersionById(int id);

    /**
     * 插入新的配置信息
     * @param engineVersionPO
     * @return
     */
    boolean insertNewEngineVersion(EngineVersionPO engineVersionPO);

    /**
     * 更新配置信息
     * @param engineVersionPO
     * @return
     */
    boolean updateEngineVersion(EngineVersionPO engineVersionPO);

    /**
     * 分页查询
     * @param pageQueryDTO
     * @return
     */
    List<EngineVersionPO> pageQuery(PageQueryDTO pageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount();

    /**
     * 分页查询
     * @param type
     * @return
     */
    List<EngineVersionPO> queryByType(String type);

}
