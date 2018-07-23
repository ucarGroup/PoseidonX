package com.ucar.flinksql.component;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Description: flexQ  输出组件
 * Created on 2018/5/7 下午2:25
 *
 */
public class FlinkFlexQOutputComponent implements FlinkComponent {

    private String name;
    private String topic;
    private String zkHostPorts;
    private String zkPrefix;
    private String[] columnNames;
    private TypeInformation<?>[] columnTypes;

    public FlinkFlexQOutputComponent(String name,String topic, String zkHostPorts,String zkPrefix ,String[] columnNames, TypeInformation<?>[] columnTypes) {

        this.name = name;
        this.topic = topic;
        this.zkHostPorts = zkHostPorts;
        this.zkPrefix = zkPrefix;

        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
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
}
