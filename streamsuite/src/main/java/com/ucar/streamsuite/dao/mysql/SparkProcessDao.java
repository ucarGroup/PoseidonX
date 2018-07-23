package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.engine.dto.ProcessPageQueryDTO;
import com.ucar.streamsuite.engine.po.JstormProcessPO;
import com.ucar.streamsuite.engine.po.SparkProcessPO;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;


/**
 * Description:spark_jstorm_process 表数据访问类
 * Created on 2018/1/18 下午4:33
 *
 */
@Repository(value="sparkProcessDao")
public interface SparkProcessDao {

    /**
     * 插入信息
     *
     * @param sparkProcessPO
     * @return
     */
    void insert(SparkProcessPO sparkProcessPO);

    /**
     * 更新信息
     *
     * @param sparkProcessPO
     * @return
     */
    void update(SparkProcessPO sparkProcessPO);

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
    SparkProcessPO getById(int id);

    /**
     * 分页查询
     * @param processPageQueryDTO
     * @return
     */
    List<SparkProcessPO> pageQuery(ProcessPageQueryDTO processPageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount(ProcessPageQueryDTO processPageQueryDTO);
}
