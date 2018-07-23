package com.ucar.streamsuite.common.hbase;

import com.ucar.streamsuite.common.hbase.vo.ColumnDescriptor;
import com.ucar.streamsuite.common.hbase.vo.TableDescriptionVo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created on 2016/6/25.
 */
public abstract class CommonHBaseTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonHBaseTable.class);

    private static final ConcurrentHashMap<String, HTable> tableMap = new ConcurrentHashMap<String, HTable>();

    private String tableName;

    //region 分裂值，默认1G
    private long FILE_SIZE = 1073741824;

    protected CommonHBaseTable(String tableName) {
        this.tableName = tableName;
    }

    /**
     * tableDescription.getListColumn().add(new ColumnDescriptor("列族名称", true));
     * tableDescription.getListColumn().add(new ColumnDescriptor());
     * @param tableDescription
     */
    public abstract void addFamily(TableDescriptionVo tableDescription);

    protected long getFileSize() {
        return FILE_SIZE;
    }

    protected long getMemStoreSize() {
        return 134217728L;
    }

    /**
     * 获取一个table autoFlush默认为true 日期为当前日志
     * @return
     */
    public HTable getHTable() {
        return getHTable(true);
    }

    /**
     * 获取一个table，同时设置autoFlush  日期为当前日志
     * @return
     */
    public HTable getHTable(boolean autoFlush) {
        SimpleDateFormat format = this.getSplitTableFormat();
        String date;
        if(format == null){
            date = "";
        } else {
            date = "_"+format.format(new Date());
        }
        String newTableName = tableName + date;
        HTable table = tableMap.get(tableName);
        if (table == null) {
            // 创建表
            table = createTableInternal(tableName,newTableName,date);
        } else {
            String tableNameRecord = table.getTableDescription().getTableName();
            if (!newTableName.equals(tableNameRecord)) {
                table.close();
                table = createTableInternal(tableName,newTableName,date);
            }
        }
        table.setAutoFlush(autoFlush);
        return table;
    }

    /**
     * 获取一个table autoFlush默认为true  可以指定日期
     * @return
     */
    public HTable getTable(Date date, boolean isCreate) throws IOException, InterruptedException {
        SimpleDateFormat format = this.getSplitTableFormat();
        String separator = format.format(date);
        String splitTableValue = "_" + separator;
        String newTable = this.tableName + splitTableValue;
        String thisSeparator = format.format(new Date());
        if(separator.equals(thisSeparator)){
            return this.getHTable();
        }
        HTable hTable = HBaseManager.getHBaseTable(newTable);
        if(hTable == null && isCreate){
            hTable = createTableInternal(newTable, splitTableValue);
        }
        return hTable;
    }

    private synchronized HTable createTableInternal(String sourceTableName,String tableName,String splitTableValue) {
        HTable table = tableMap.get(sourceTableName);
        if (table != null) {
            if(tableName.equals(table.getTableDescription().getTableName())) {
                return table;
            }
        }
        table = createTableInternal(tableName,splitTableValue);
        tableMap.put(sourceTableName,table);
        return table;
    }

    private synchronized HTable createTableInternal(String tableName,String splitTableValue) {
        LOGGER.info("create table : " + tableName);

        TableDescriptionVo tableDescription = new TableDescriptionVo();
        tableDescription.setTableName(tableName);
        tableDescription.setListColumn(new ArrayList<ColumnDescriptor>());
        tableDescription.setFileSize(getFileSize());
        tableDescription.setMemStoreSize(getMemStoreSize());
        addFamily(tableDescription);
        HTable table = new HTable(tableDescription, splitTableValue);
        return table;
    }

    /**
     * 默认按照月分表，返回空不做分表处理
     */
    protected SimpleDateFormat getSplitTableFormat(){
        return new SimpleDateFormat("yyyyMM");
    }



}
