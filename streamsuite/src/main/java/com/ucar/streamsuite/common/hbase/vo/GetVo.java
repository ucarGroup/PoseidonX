package com.ucar.streamsuite.common.hbase.vo;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

public class GetVo extends CommonHbaseVo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tableName ;

    private String rowKey;

    private byte[] byteRowKey;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRowKey() {
        return rowKey;
    }

    @Deprecated
    public void setRowKey(String rowKey) {
        this.byteRowKey = Bytes.toBytes(rowKey);
        this.rowKey = rowKey;
    }

    public byte[] getByteRowKey() {
        if (byteRowKey != null) {
            return byteRowKey;
        } else if (rowKey != null) {
            return Bytes.toBytes(rowKey);
        } else {
            return null;
        }
    }

    public void setByteRowKey(byte[] byteRowKey) {
        this.byteRowKey = byteRowKey;
    }
}
