package org.apache.flink.streaming.connectors.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ucar.flinksql.ConstantForSQL;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.mortbay.log.Log;

import java.io.IOException;

/**
 * Description:
 * Created on 2018/5/30 下午4:51
 *
 *
 */
public class MyJsonRowDeserializationSchema implements DeserializationSchema<Row> {

    /** Type information describing the result type. */
    private final TypeInformation<Row> typeInfo;

    /** Field names to parse. Indices match fieldTypes indices. */
    private final String[] fieldNames;

    /** Types to parse fields as. Indices match fieldNames indices. */
    private final TypeInformation<?>[] fieldTypes;

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Flag indicating whether to fail on a missing field. */
    private boolean failOnMissingField;

    private String[] columnNames; //源列名

    /**
     * Creates a JSON deserialization schema for the given fields and types.
     *
     * @param typeInfo   Type information describing the result type. The field names are used
     *                   to parse the JSON file and so are the types.
     * @param columnNames
     */
    public MyJsonRowDeserializationSchema(TypeInformation<Row> typeInfo, String[] columnNames) {
        Preconditions.checkNotNull(typeInfo, "Type information");
        this.typeInfo = typeInfo;

        this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();
        this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();
        this.columnNames = columnNames;

    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            JsonNode root = objectMapper.readTree(message);

            Row row = new Row(fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {

                //数据源本来的字段名
                String columnName = columnNames[i];

                JsonNode node = root.get(columnName);

                if (node == null) {
                    if (failOnMissingField) {
                        throw new IllegalStateException("Failed to find field with name '"
                                + fieldNames[i] + "'.");
                    } else {
                        row.setField(i, null);
                    }
                } else {
                    // Read the value as specified type
                    try {
                        Object value = objectMapper.treeToValue(node, fieldTypes[i].getTypeClass());
                        row.setField(i, value);
                    }catch (Exception e){
                        Log.warn("Failed to deserialize JSON object.[" + new String(message, "UTF-8") + "]", e);
                        row.setField(i, null);
                    }

                }
            }

            return row;
        } catch (Throwable t) {
            Log.warn("Failed to deserialize JSON object.[" + new String(message, "UTF-8") + "]", t);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }

    /**
     * Configures the failure behaviour if a JSON field is missing.
     *
     * <p>By default, a missing field is ignored and the field is set to null.
     *
     * @param failOnMissingField Flag indicating whether to fail or not on a missing field.
     */
    public void setFailOnMissingField(boolean failOnMissingField) {
        this.failOnMissingField = failOnMissingField;
    }

}