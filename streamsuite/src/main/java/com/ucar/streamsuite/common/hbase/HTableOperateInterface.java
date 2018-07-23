package com.ucar.streamsuite.common.hbase;

import com.ucar.streamsuite.common.hbase.vo.*;

import java.io.IOException;
import java.util.List;

/**
 * Created on 2017/3/24.
 * Description : HBase操作接口
 */
public interface HTableOperateInterface {

    void createTable(TableDescriptionVo vo) throws IOException;

    void put(HbaseRemoteVo vo);

    RowVo get(GetVo vo);

    List<RowVo> get(List<GetVo> getVos);

    List<RowVo> scan(ScanVo vo);

    boolean tableExists(String tableName);

    boolean tableExists(String clusterName, String tableName);

    void delete(DeleteVo deleteVo);

    void delete(List<DeleteVo> deleteVos);

    long increase(IncrementVo incrementVo) throws Exception;

}
