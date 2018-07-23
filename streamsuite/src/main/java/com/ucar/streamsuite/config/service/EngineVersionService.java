package com.ucar.streamsuite.config.service;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.config.po.EngineVersionPO;

import java.util.List;

/**
 * Description:
 * Created on 2018/2/2 下午6:18
 *
 *
 */
public interface EngineVersionService {

    PageResultDTO pageQuery(PageQueryDTO pageQueryDTO);

    EngineVersionPO getEngineVersionById(Integer engineVersionId);

    boolean insertEngineVersion(EngineVersionPO engineVersionPO);

    void updateEngineVersion(EngineVersionPO engineVersionPO);

    List<EngineVersionPO> queryByType (String engineVersionType);


}
