package com.ucar.streamsuite.common.hbase.vo;

import java.io.Serializable;

/**
 * Created   on 2016/11/2.
 * Description : 自增vo
 */
public class IncrementVo extends CommonHbaseVo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tableName;

    private byte[] rowKey;

    private byte[] family;

    private byte[] qualifier;

    private long incrementValue = 1L;

    public IncrementVo(byte[] rowKey, byte[] family, byte[] qualifier) {
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
    }

    public IncrementVo(byte[] rowKey, byte[] family, byte[] qualifier, long incrementValue) {
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
        this.incrementValue = incrementValue;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public byte[] getFamily() {
        return family;
    }

    public void setFamily(byte[] family) {
        this.family = family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public void setQualifier(byte[] qualifier) {
        this.qualifier = qualifier;
    }

    public long getIncrementValue() {
        return incrementValue;
    }

    public void setIncrementValue(long incrementValue) {
        this.incrementValue = incrementValue;
    }

}
