package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.engine.dto.ProcessPageQueryDTO;
import com.ucar.streamsuite.engine.po.FlinkProcessPO;
import com.ucar.streamsuite.engine.po.JstormProcessPO;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Repository;


/**
 * Description:stream_jstorm_process 表数据访问类
 * Created on 2018/1/18 下午4:33
 *
 */
@Repository(value="jstormProcessDao")
public interface JstormProcessDao {

    /**
     * 插入信息
     *
     * @param jstormProcessPO
     * @return
     */
    void insert(JstormProcessPO jstormProcessPO);

    /**
     * 更新信息
     *
     * @param jstormProcessPO
     * @return
     */
    void update(JstormProcessPO jstormProcessPO);

    /**
     * 更新提交结果
     *
     * @return
     */
    void updateSubmitOk(int id);

    /**
     * 根据id获取信息
     *
     * @param id
     * @return
     */
    JstormProcessPO getById(int id);

    /**
     * 根据id获取信息
     *
     * @return
     */
    List<JstormProcessPO> getByIds(Map<String,Object> params);
    /**
     * 分页查询
     * @param processPageQueryDTO
     * @return
     */
    List<JstormProcessPO> pageQuery(ProcessPageQueryDTO processPageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount(ProcessPageQueryDTO processPageQueryDTO);
}
