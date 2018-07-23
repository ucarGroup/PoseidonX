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

package com.huawei.streaming.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.huawei.streaming.util.StreamingUtils;
import org.apache.commons.lang.StringUtils;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.exception.StreamingException;

/**
 * 功能性算子的接口
 * 
 */
public abstract class FunctionOperator extends AbsOperator
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -6173328334801673022L;
    
    private IEventType outputSchema;
    
    private String outputStreamName;
    
    private List<String> inputStreams;
    
    private Map<String, IEventType> inputSchemas;
    
    /**
     * <默认构造函数>
     */
    public FunctionOperator()
    {
        inputStreams = new ArrayList<String>();
        inputSchemas = new HashMap<String, IEventType>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException
    {
        super.setConfig(conf);
        this.addInputStream((String)conf.get(StreamingConfig.STREAMING_INNER_INPUT_STREAM_NAME));
        this.addInputSchema((String)conf.get(StreamingConfig.STREAMING_INNER_INPUT_STREAM_NAME),
                StreamingUtils.deSerializeSchema((String) conf.get(StreamingConfig.STREAMING_INNER_INPUT_SCHEMA)));
        this.outputSchema = StreamingUtils.deSerializeSchema((String)(conf.get(StreamingConfig.STREAMING_INNER_OUTPUT_SCHEMA)));
        this.outputStreamName = (String)conf.get(StreamingConfig.STREAMING_INNER_OUTPUT_STREAM_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setOutputStream(String streamName)
    {
        this.outputStreamName = streamName;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getOutputStream()
    {
        return outputStreamName;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setOutputSchema(IEventType schema)
    {
        this.outputSchema = schema;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IEventType getOutputSchema()
    {
        return this.outputSchema;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setInputStream(List<String> streamNames)
    {
        this.inputStreams = streamNames;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getInputStream()
    {
        return inputStreams;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setInputSchema(Map<String, IEventType> schemas)
    {
        this.inputSchemas = schemas;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, IEventType> getInputSchema()
    {
        return this.inputSchemas;
    }
    
    /**
     * 添加输入流
     */
    public void addInputStream(String streamName)
    {
        if (!StringUtils.isEmpty(streamName))
        {
            if (!inputStreams.contains(streamName))
            {
                inputStreams.add(streamName);
            }
        }
    }
    
    /**
     * 添加输入流schema
     */
    public void addInputSchema(String streamName, IEventType schema)
    {
        if (schema != null)
        {
            inputSchemas.put(streamName, schema);
        }
    }
    
}
