package com.ucar.streamsuite.dao.mysql;

import com.ucar.streamsuite.engine.dto.ProcessPageQueryDTO;
import com.ucar.streamsuite.engine.po.FlinkProcessPO;
import com.ucar.streamsuite.engine.po.JstormProcessPO;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * Description:stream_flink_process 表数据访问类
 * Created on 2018/1/18 下午4:33
 *
 */
@Repository(value="FlinkProcessDao")
public interface FlinkProcessDao {

    /**
     * 插入信息
     *
     * @param flinkProcessPO
     * @return
     */
    void insert(FlinkProcessPO flinkProcessPO);

    /**
     * 更新信息
     *
     * @param flinkProcessPO
     * @return
     */
    void update(FlinkProcessPO flinkProcessPO);

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
    FlinkProcessPO getById(int id);

    /**
     * 根据id获取信息
     *
     * @return
     */
    List<FlinkProcessPO> getByIds(Map<String,Object> params);

    /**
     * 分页查询
     * @param processPageQueryDTO
     * @return
     */
    List<FlinkProcessPO> pageQuery(ProcessPageQueryDTO processPageQueryDTO);

    /**
     * 查询总数
     * @return
     */
    Integer queryCount(ProcessPageQueryDTO processPageQueryDTO);

}
