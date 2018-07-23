package com.ucar.flinksql.userFunction;

import com.ucar.streamsuite.common.hbase.util.HBaseUtils;
import org.apache.flink.table.functions.ScalarFunction;
import java.sql.Timestamp;
/**
 * Description: 用于hbase rowkey 时生成 当前时间 倒序的时间戳
 * Created on 2018/6/13 下午7:41
 *
 */
public class HBaseTimeDesc extends ScalarFunction {

    public Long  eval(Timestamp timestamp) {
        return HBaseUtils.getThisTimeDesc(timestamp.getTime());
    }
}
