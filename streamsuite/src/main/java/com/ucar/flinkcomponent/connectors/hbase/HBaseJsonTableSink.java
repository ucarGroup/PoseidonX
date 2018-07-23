package com.ucar.flinkcomponent.connectors.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Description: 支持写数据到hbase
 * Created on 2018/6/12 上午10:30
 *
 *
 */
public class HBaseJsonTableSink  implements AppendStreamTableSink<Row> {

    private final Properties properties;
    protected String[] fieldNames;
    protected TypeInformation[] fieldTypes;

    public HBaseJsonTableSink(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        HBaseSinkForRow hBaseSinkForRow = new HBaseSinkForRow(properties,fieldNames,fieldTypes);
        dataStream.addSink(hBaseSinkForRow);
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(getFieldTypes());
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
            return fieldTypes ;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {

        HBaseJsonTableSink hBaseJsonTableSink = new HBaseJsonTableSink(properties);
        hBaseJsonTableSink.fieldNames = fieldNames;
        hBaseJsonTableSink.fieldTypes = fieldTypes;

        return hBaseJsonTableSink;
    }
}
