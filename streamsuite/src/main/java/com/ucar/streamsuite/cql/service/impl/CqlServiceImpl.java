package com.ucar.streamsuite.cql.service.impl;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.util.StreamUtil;
import com.ucar.streamsuite.cql.dto.CqlDTO;
import com.ucar.streamsuite.cql.po.CqlPO;
import com.ucar.streamsuite.cql.service.CqlService;
import com.ucar.streamsuite.dao.mysql.CqlDao;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Description:
 * Created on 2018/3/12 上午10:22
 *
 */
@Service
public class CqlServiceImpl implements CqlService{

    @Resource
    private CqlDao cqlDao;

    @Override
    public PageResultDTO pageQuery(PageQueryDTO pageQueryDTO) {
        PageResultDTO pageResultDTO = new PageResultDTO();
        pageQueryDTO = StreamUtil.dealPageQuery(pageQueryDTO);

        //查询列表
        pageResultDTO.setList(cqlDao.pageQuery(pageQueryDTO));

        //查询总数
        pageResultDTO.setCount(cqlDao.queryCount());
        pageResultDTO.setCurrentPage(pageQueryDTO.getPageNum());
        return pageResultDTO;
    }

    @Override
    public CqlPO getCqlById(Integer id) {
        return cqlDao.getCqlById(id);
    }

    @Override
    public boolean insertCql(CqlPO cqlPO) {
        return cqlDao.insertNewCql(cqlPO);
    }

    @Override
    public void updateCql(CqlPO cqlPO) {
        cqlDao.updateCql(cqlPO);
    }

    @Override
    public List<CqlPO> getCqlByUser(String userName,String cqlType) {
        Map<String,String> params = new HashMap<String,String>();
        params.put("userName",userName);
        params.put("cqlType",cqlType);
        return cqlDao.getCqlByUser(params);
    }
}
