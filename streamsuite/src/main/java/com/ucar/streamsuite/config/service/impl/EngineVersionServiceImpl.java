package com.ucar.streamsuite.config.service.impl;

import com.ucar.streamsuite.common.constant.CommonStatusEnum;
import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.util.StreamUtil;

import com.ucar.streamsuite.config.po.EngineVersionPO;
import com.ucar.streamsuite.config.service.EngineVersionService;
import com.ucar.streamsuite.dao.mysql.EngineVersionDao;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;

/**
 * Description:
 * Created on 2018/2/2 下午6:18
 *
 *
 */
@Service
public class EngineVersionServiceImpl implements EngineVersionService {

    @Resource
    private EngineVersionDao engineVersionDao;


    @Override
    public PageResultDTO pageQuery(PageQueryDTO pageQueryDTO) {
        PageResultDTO pageResultDTO = new PageResultDTO();

        pageQueryDTO = StreamUtil.dealPageQuery(pageQueryDTO);

        //查询列表
        pageResultDTO.setList(engineVersionDao.pageQuery(pageQueryDTO));

        //查询总数
        pageResultDTO.setCount(engineVersionDao.queryCount());

        pageResultDTO.setCurrentPage(pageQueryDTO.getPageNum());

        return pageResultDTO;
    }

    @Override
    public EngineVersionPO getEngineVersionById(Integer engineVersionId) {
        return engineVersionDao.getEngineVersionById(engineVersionId);
    }

    @Override
    public boolean insertEngineVersion(EngineVersionPO engineVersionPO) {
        engineVersionPO.setStatus(CommonStatusEnum.ENABLE.ordinal());
        engineVersionPO.setCreateTime(new Date());
        return engineVersionDao.insertNewEngineVersion(engineVersionPO);
    }

    @Override
    public void updateEngineVersion(EngineVersionPO engineVersionPO) {
        engineVersionPO.setModifyTime(new Date());
        engineVersionDao.updateEngineVersion(engineVersionPO);
    }

    @Override
    public List<EngineVersionPO> queryByType(String engineVersionType) {
        return engineVersionDao.queryByType(engineVersionType);
    }


}
