package com.ucar.flinksql.component;

import com.ucar.flinkcomponent.connectors.hbase.HBaseConstant;
import com.ucar.flinksql.ConstantForSQL;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Description:
 * Created on 2018/6/13 下午5:13
 *
 */
public class FlinkHBaseOutputComponent implements FlinkComponent {

    private String name;
    private String zkHost;
    private String zkPort;
    private String zkPrefix;
    private String tableName;
    private String colFamily;

    String[] columnNames;
    TypeInformation[] dataTypeStrArray;


    public String getZkHost() {
        return zkHost;
    }

    public void setZkHost(String zkHost) {
        this.zkHost = zkHost;
    }

    public String getZkPort() {
        return zkPort;
    }

    public void setZkPort(String zkPort) {
        this.zkPort = zkPort;
    }

    public String getZkPrefix() {
        return zkPrefix;
    }

    public void setZkPrefix(String zkPrefix) {
        this.zkPrefix = zkPrefix;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColFamily() {
        return colFamily;
    }

    public void setColFamily(String colFamily) {
        this.colFamily = colFamily;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public TypeInformation[] getDataTypeStrArray() {
        return dataTypeStrArray;
    }

    public void setDataTypeStrArray(TypeInformation[] dataTypeStrArray) {
        this.dataTypeStrArray = dataTypeStrArray;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
