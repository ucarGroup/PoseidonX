package com.ucar.streamsuite.common.hbase.vo;

import java.io.Serializable;

public class DeleteVo extends CommonHbaseVo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tableName;

    private byte[] rowKey;

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
