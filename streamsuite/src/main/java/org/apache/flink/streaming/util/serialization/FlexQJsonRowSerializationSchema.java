package org.apache.flink.streaming.util.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * Description:
 * Created on 2018/5/10 下午4:17
 *
 *
 */
public class FlexQJsonRowSerializationSchema implements SerializationSchema<Row> {
    /** Fields names in the input Row object */
    private final String[] fieldNames;
    /** Object mapper that is used to create output JSON objects */
    private static ObjectMapper mapper = new ObjectMapper();

    /**
     * Creates a JSON serialization schema for the given fields and types.
     *
     * @param fieldNames Names of JSON fields to parse.
     */
	public FlexQJsonRowSerializationSchema(String[] fieldNames) {
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
    }

    @Override
    public byte[] serialize(Row row) {
        if (row.getArity() != fieldNames.length) {
            throw new IllegalStateException(String.format(
                    "Number of elements in the row %s is different from number of field names: %d", row, fieldNames.length));
        }

        ObjectNode objectNode = mapper.createObjectNode();

        for (int i = 0; i < row.getArity(); i++) {
            JsonNode node = mapper.valueToTree(row.getField(i));
            objectNode.set(fieldNames[i], node);
        }

        try {
            return mapper.writeValueAsBytes(objectNode);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize row", e);
        }
    }
}