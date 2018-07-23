package com.ucar.flinksql.component;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Description:flexq 输入组件
 * Created on 2018/5/7 下午2:25
 *
 */
public class FlinkFlexQInputComponent implements FlinkComponent {

    private String name;
    private String topic;
    private String zkHostPorts;
    private String zkPrefix;
    private String group;
    private String format;
    private String[] columnNames; //sourceColName
    private String[] columnAliasNames; //在sql中真正使用的列名
    private TypeInformation<?>[] columnTypes;

    public FlinkFlexQInputComponent(String name,String topic, String zkHostPorts,String zkPrefix, String group, String format,String[] columnNames, String[] columnAliasNames,TypeInformation<?>[] columnTypes) {
        this.name = name;
        this.topic = topic;
        this.zkHostPorts = zkHostPorts;
        this.zkPrefix = zkPrefix;
        this.group = group;
        this.columnNames = columnNames;
        this.columnAliasNames = columnAliasNames;
        this.columnTypes = columnTypes;
        this.format = format;

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getZkHostPorts() {
        return zkHostPorts;
    }

    public void setZkHostPorts(String zkHostPorts) {
        this.zkHostPorts = zkHostPorts;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public TypeInformation<?>[] getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(TypeInformation<?>[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    public String getZkPrefix() {
        return zkPrefix;
    }

    public void setZkPrefix(String zkPrefix) {
        this.zkPrefix = zkPrefix;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[] getColumnAliasNames() {
        return columnAliasNames;
    }

    public void setColumnAliasNames(String[] columnAliasNames) {
        this.columnAliasNames = columnAliasNames;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
