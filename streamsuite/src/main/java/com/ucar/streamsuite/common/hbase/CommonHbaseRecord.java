package com.ucar.streamsuite.common.hbase;

import com.ucar.streamsuite.common.hbase.util.HBaseUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CommonHbaseRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tableName;
    private String familyName;
    private String rowKey;
    private long time;
    private List<HbaseCellItem> hbaseCellList = new ArrayList<HbaseCellItem>();

    public CommonHbaseRecord(){
    }

    public CommonHbaseRecord(String tableName, String familyName, String rowKey){
           this.tableName = tableName;
           this.familyName = familyName;
           this.rowKey = rowKey;
           this.time = System.currentTimeMillis();
    }

    public CommonHbaseRecord(String tableName, String rowKey){
        this.tableName = tableName;
        this.familyName = HBaseUtils.DEFAULT_FAMILIY_NAME;
        this.rowKey = rowKey;
        this.time = System.currentTimeMillis();
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public List<HbaseCellItem> getHbaseCellList() {
        return hbaseCellList;
    }

    public void setHbaseCellList(List<HbaseCellItem> hbaseCellList) {
        this.hbaseCellList = hbaseCellList;
    }

    public void addHbaseCell(String columnName,byte[] value){
        this.hbaseCellList.add(new HbaseCellItem(familyName,columnName,value));
    }

    public CommonHbaseRecord duplicateWithoutItem(){
        return  new CommonHbaseRecord(tableName,familyName,rowKey);
    }

    public class HbaseCellItem implements Serializable{

        private static final long serialVersionUID = 1L;

        private String familyName;
        private String columnName;
        private byte[] value;

        public HbaseCellItem(String familyName,String columnName,byte[] value){
            this.familyName = familyName;
            this.columnName = columnName;
            this.value = value;
        }

        public String getFamilyName() {
            return familyName;
        }

        public void setFamilyName(String familyName) {
            this.familyName = familyName;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public byte[] getValue() {
            return value;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }
    }
}
