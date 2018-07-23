/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.streaming.cql.semanticanalyzer;

import java.util.Map;

import com.google.common.collect.Maps;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.CreateStreamAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ColumnNameTypeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ColumnNameTypeListContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.KeyValuePropertyContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.StreamPropertiesContext;

/**
 * create stream语义分析
 * 
 */
public abstract class CreateStreamAnalyzer extends BaseAnalyzer
{
    private CreateStreamAnalyzeContext analyzeContext = null;
    
    /**
     * <默认构造函数>
     */
    public CreateStreamAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        analyzeContext = new CreateStreamAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected CreateStreamAnalyzeContext getAnalyzeContext()
    {
        return analyzeContext;
    }
    
    /**
     * 创建schema
     */
    protected Schema createSchema(String streamName, ColumnNameTypeListContext columns)
    {
        Schema schema = new Schema(streamName);
        for (ColumnNameTypeContext column : columns.getColumns())
        {
            schema.addCol(new Column(column.getColumnName(), column.getColumnType().getWrapperClass()));
        }
        return schema;
    }
    
    /**
     * 分析流属性
     */
    protected Map<String, String> analyzeStreamProperties(StreamPropertiesContext streamPropertiesContext)
    {
        if (streamPropertiesContext == null)
        {
            return Maps.newHashMap();
        }
        
        return parseStreamProperties(streamPropertiesContext);
    }
    
    private Map<String, String> parseStreamProperties(StreamPropertiesContext streamPropertiesContext)
    {
        Map<String, String> properties = Maps.newHashMap();
        for (KeyValuePropertyContext ctx : streamPropertiesContext.getProperties())
        {
            String key = ctx.getKey();
            String value = ctx.getValue();
            properties.put(key, value);
        }
        return properties;
    }
    
}
