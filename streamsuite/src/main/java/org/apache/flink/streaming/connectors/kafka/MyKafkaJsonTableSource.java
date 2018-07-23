package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowDeserializationSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Description:
 * Created on 2018/5/30 下午4:47
 *
 *
 */
public class MyKafkaJsonTableSource extends KafkaTableSource {

    private String[] columnNames; //源列名

    /**
     * Creates a generic Kafka JSON {@link StreamTableSource}.
     *  @param topic      Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param typeInfo   Type information describing the result type. The field names are used
     * @param columnNames
     */
    public MyKafkaJsonTableSource(
            String topic,
            Properties properties,
            TypeInformation<Row> typeInfo, String[] columnNames) {

        super(topic, properties, createDeserializationSchema(typeInfo,columnNames), typeInfo);
        this.columnNames = columnNames;
    }

    /**
     * Configures the failure behaviour if a JSON field is missing.
     *
     * <p>By default, a missing field is ignored and the field is set to null.
     *
     * @param failOnMissingField Flag indicating whether to fail or not on a missing field.
     */
    public void setFailOnMissingField(boolean failOnMissingField) {
        JsonRowDeserializationSchema deserializationSchema = (JsonRowDeserializationSchema) getDeserializationSchema();
        deserializationSchema.setFailOnMissingField(failOnMissingField);
    }

    private static MyJsonRowDeserializationSchema createDeserializationSchema(TypeInformation<Row> typeInfo,String[] columnNames) {

        return new MyJsonRowDeserializationSchema(typeInfo,columnNames);
    }

    @Override
    FlinkKafkaConsumerBase<Row> getKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
        return new FlinkKafkaConsumer010<>(topic, deserializationSchema, properties);
    }

    public String[] getColumnNames() {
        return columnNames;
    }
}
