package com.ucar.flinksql.component;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Description: kafka 09 输出数据 组件
 * Created on 2018/5/7 下午2:25
 *
 */
public class FlinkKafka09OutputComponent implements FlinkComponent {

    private String name;
    private String topic;
    private String brokerHostPorts;
    private String[] columnNames;
    private TypeInformation<?>[] columnTypes;

    public FlinkKafka09OutputComponent(String name,String topic, String brokerHostPorts, String[] columnNames, TypeInformation<?>[] columnTypes) {
        this.name = name;
        this.topic = topic;
        this.brokerHostPorts = brokerHostPorts;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerHostPorts() {
        return brokerHostPorts;
    }

    public void setBrokerHostPorts(String brokerHostPorts) {
        this.brokerHostPorts = brokerHostPorts;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
