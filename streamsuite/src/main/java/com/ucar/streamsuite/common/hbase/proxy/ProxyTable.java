package com.ucar.streamsuite.common.hbase.proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Created by on 2017/3/24.
 * Description :
 */
public class ProxyTable extends HTable {

    //最后更新时间
    private volatile long updateTime = System.currentTimeMillis();
    //是否失效，失效关闭连接，并移除pool
    private volatile boolean isFail = false ;
    //ProxyTableHolder uuid
    private volatile String holderUUID;

    public ProxyTable(Configuration conf, final String tableName,String holderUUID)
            throws IOException {
        super(conf, TableName.valueOf(tableName));
        this.holderUUID = holderUUID;
    }

    public ProxyTable(Configuration conf, final byte[] tableName,String holderUUID)
            throws IOException {
        super(conf, tableName);
        this.holderUUID = holderUUID;
    }

    public boolean isFail() {
        return isFail;
    }

    public void setFail(boolean isFail) {
        this.isFail = isFail;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public String getHolderUUID() {
        return holderUUID;
    }

    public void setHolderUUID(String holderUUID) {
        this.holderUUID = holderUUID;
    }
}
