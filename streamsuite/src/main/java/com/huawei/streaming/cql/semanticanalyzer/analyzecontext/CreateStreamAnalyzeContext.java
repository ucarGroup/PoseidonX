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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * 创建新流语义分析内容
 * 
 */
public class CreateStreamAnalyzeContext extends AnalyzeContext
{
    private Schema schema;
    
    private String streamName;
    
    private String streamAlias;
    
    private String deserializerClassName;
    
    private String serializerClassName;
    
    private String recordReaderClassName;
    
    private String recordWriterClassName;
    
    private Integer parallelNumber;
    
    private Map<String, String> serDeProperties = new HashMap<String, String>();
    
    private Map<String, String> readWriterProperties = new HashMap<String, String>();
    
    private ParseContext parseContext = null;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParseContext(ParseContext parseContext)
    {
        this.parseContext = parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void validateParseContext()
        throws SemanticAnalyzerException
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<Schema> getCreatedSchemas()
    {
        return Lists.newArrayList(schema);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        if (parseContext == null)
        {
            return "";
        }
        return parseContext.toString();
    }
    
    public String getStreamName()
    {
        return streamName;
    }
    
    public void setStreamName(String streamName)
    {
        this.streamName = streamName;
    }
    
    public String getStreamAlias()
    {
        return streamAlias;
    }
    
    public void setStreamAlias(String streamAlias)
    {
        this.streamAlias = streamAlias;
    }
    
    public String getDeserializerClassName()
    {
        return deserializerClassName;
    }
    
    public void setDeserializerClassName(String deserializerClassName)
    {
        this.deserializerClassName = deserializerClassName;
    }
    
    public String getSerializerClassName()
    {
        return serializerClassName;
    }
    
    public void setSerializerClassName(String serializerClassName)
    {
        this.serializerClassName = serializerClassName;
    }
    
    public String getRecordReaderClassName()
    {
        return recordReaderClassName;
    }
    
    public void setRecordReaderClassName(String recordReaderClassName)
    {
        this.recordReaderClassName = recordReaderClassName;
    }
    
    public String getRecordWriterClassName()
    {
        return recordWriterClassName;
    }
    
    public void setRecordWriterClassName(String recordWriterClassName)
    {
        this.recordWriterClassName = recordWriterClassName;
    }
    
    public Integer getParallelNumber()
    {
        return parallelNumber;
    }
    
    public void setParallelNumber(Integer parallelNumber)
    {
        this.parallelNumber = parallelNumber;
    }
    
    public Map<String, String> getSerDeProperties()
    {
        return serDeProperties;
    }
    
    public void setSerDeProperties(Map<String, String> serDeProperties)
    {
        this.serDeProperties = serDeProperties;
    }
    
    public Map<String, String> getReadWriterProperties()
    {
        return readWriterProperties;
    }
    
    public void setReadWriterProperties(Map<String, String> readWriterProperties)
    {
        this.readWriterProperties = readWriterProperties;
    }
    
    public Schema getSchema()
    {
        return schema;
    }
    
    public void setSchema(Schema schema)
    {
        this.schema = schema;
    }
}
