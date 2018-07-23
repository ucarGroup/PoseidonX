package com.ucar.flinksql.component;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Description: kafka 010 输入组件
 * Created on 2018/5/7 下午2:25
 *
 */
public class FlinkKafka010InputComponent implements FlinkComponent {

    private String name;
    private String topic;
    private String brokerHostPorts;
    private String group;
    private String[] columnNames; //sourceColName
    private String[] columnAliasNames; //在sql中真正使用的列名
    private TypeInformation<?>[] columnTypes;

    public FlinkKafka010InputComponent(String name,String topic, String brokerHostPorts,String group, String[] columnNames, String[] columnAliasNames,TypeInformation<?>[] columnTypes) {
        this.name = name;
        this.topic = topic;
        this.brokerHostPorts = brokerHostPorts;
        this.group = group;
        this.columnNames = columnNames;
        this.columnAliasNames = columnAliasNames;
        this.columnTypes = columnTypes;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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

    public String getBrokerHostPorts() {
        return brokerHostPorts;
    }

    public void setBrokerHostPorts(String brokerHostPorts) {
        this.brokerHostPorts = brokerHostPorts;
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
}
