package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.cql.po.CqlPO;


import java.util.List;
import java.util.Map;

/**
 * Description: cql 脚本 DAO
 * Created on 2018/3/12 上午9:30
 *
 */
public interface CqlDao {

    /**
     * 通过id获取cql信息
     * @param id
     * @return
     */
    CqlPO getCqlById(int id);

    /**
     * 插入新的cql信息
     * @param cqlPO
     * @return
     */
    boolean insertNewCql(CqlPO cqlPO);

    /**
     * 更新的cql信息
     * @param cqlPO
     * @return
     */
    boolean updateCql(CqlPO cqlPO);

    /**
     * 分页查询
     * @param pageQueryDTO
     * @return
     */
    List<CqlPO> pageQuery(PageQueryDTO pageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount();

    /**
     * 通过用户查询CQL脚本
     * @return
     */
    List<CqlPO> getCqlByUser(Map<String,String> params);
}
