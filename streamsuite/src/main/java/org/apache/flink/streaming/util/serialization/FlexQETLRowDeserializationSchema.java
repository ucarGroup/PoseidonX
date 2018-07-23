/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ucar.flinksql.ConstantForSQL;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Iterator;

/**
 * * Description: 这个数据源专门用于处理接收数据同步系统datalink过来的数据
 * 结构类似于
 *{
 "databaseName": "scar_order",
 "dbTableRowCellVOList": [
 {
 "afterValue": "2501504660010",
 "beforeValue": "2501504660010",
 "columnName": "order_no"
 },
 {
 "afterValue": "442763",
 "beforeValue": "442763",
 "columnName": "member_id"
 },
 {
 "afterValue": "6564598929508401153",
 "columnName": "id"
 }
 ],
 "eventType": "UPDATE",
 "id": "6564598929508401153",
 "tableName": "t_sco_order"
 }

 处理成
 {
 "databaseName": "scar_order",
 "tableName": "t_sco_order",
 "eventType": "UPDATE",
 "id"
 *
 * Deserialization schema from JSON to {@link Row}.
 *
 * <p>Deserializes the <code>byte[]</code> messages as a JSON object and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
public class FlexQETLRowDeserializationSchema implements DeserializationSchema<Row> {

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
    public FlexQETLRowDeserializationSchema(TypeInformation<Row> typeInfo, String[] columnNames) {
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


                //处理 row_before_xxx 和 row_after_xxx
                if(columnName.startsWith(ConstantForSQL.ETL_ROW_BEFORE)){
                    JsonNode node = root.get(ConstantForSQL.ETL_ROW_LIST);

                    Iterator<JsonNode> elements = node.elements();

                    while (elements.hasNext()){
                        JsonNode jsonNode = elements.next();
                        String tableColumnName =  jsonNode.get("columnname").asText();

                        if((ConstantForSQL.ETL_ROW_BEFORE + tableColumnName).equals(columnName)){
                            String beforeValue =  jsonNode.get("beforevalue").asText();
                            row.setField(i, beforeValue);
                        }

                    }

                }
                else if(columnName.startsWith(ConstantForSQL.ETL_ROW_AFTER)){
                    JsonNode node = root.get(ConstantForSQL.ETL_ROW_LIST);

                    Iterator<JsonNode> elements = node.elements();

                    while (elements.hasNext()){
                        JsonNode jsonNode = elements.next();
                        String tableColumnName =  jsonNode.get("columnname").asText();

                        if((ConstantForSQL.ETL_ROW_AFTER + tableColumnName).equals(columnName)){

                            String afterValue =  jsonNode.get("aftervalue").asText();
                            row.setField(i, afterValue);
                        }

                    }
                }
                else {
                    //处理其他
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
                        Object value = objectMapper.treeToValue(node, fieldTypes[i].getTypeClass());
                        row.setField(i, value);
                    }
                }
            }

            return row;
        } catch (Throwable t) {
            throw new IOException("Failed to deserialize JSON object.", t);
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
