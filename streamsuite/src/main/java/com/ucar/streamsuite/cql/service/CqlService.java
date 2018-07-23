package com.ucar.streamsuite.cql.service;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.cql.po.CqlPO;
import com.ucar.streamsuite.task.po.TaskArchivePO;

import java.util.List;

/**
 * Description:
 * Created on 2018/3/12 上午10:21
 *
 */
public interface CqlService {

    CqlPO getCqlById(Integer id);

    PageResultDTO pageQuery(PageQueryDTO pageQueryDTO);

    boolean insertCql(CqlPO cqlPO);

    void updateCql(CqlPO cqlPO);

    List<CqlPO> getCqlByUser(String userName,String cqlType);

}
