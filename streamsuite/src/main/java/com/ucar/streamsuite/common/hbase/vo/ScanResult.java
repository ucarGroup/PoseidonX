package com.ucar.streamsuite.common.hbase.vo;

import java.io.IOException;

/**
 * Created   on 2016/8/22.
 */
public interface ScanResult extends Iterable<RowVo> {

    RowVo next() throws IOException;

    RowVo[] next(int nbRows) throws IOException;

}
