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

import java.util.List;
import java.util.Map;

import com.huawei.streaming.util.StreamingUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * 最基础的输出算子
 * 
 */
public final class OutputOperator extends AbsOperator
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 6316041458965215046L;

    private static final Logger LOG = LoggerFactory.getLogger(OutputOperator.class);

    /**
     * 序列化类
     */
    private StreamSerDe serde;
    
    private List<String> inputStreams;
    
    private Map<String, IEventType> inputSchemas;

    private IOutputStreamOperator outputStream;

    /**
     * <默认构造函数>
     */
    public OutputOperator()
    {
        inputStreams = Lists.newArrayList();
        inputSchemas = Maps.newHashMap();
    }

    /**
     * 设置输出流算子
     */
    public void setOutputStreamOperator(IOutputStreamOperator stream)
    {
        this.outputStream = stream;
    }

    /**
     * 获取输出流算子
     */
    public IOutputStreamOperator getOutputStreamOperator()
    {
        return this.outputStream;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException
    {
        super.setConfig(conf);
        TupleEventType tupleEventType = StreamingUtils.deSerializeSchema((String)conf.get(StreamingConfig.STREAMING_INNER_INPUT_SCHEMA));

        this.addInputStream((String)conf.get(StreamingConfig.STREAMING_INNER_INPUT_STREAM_NAME));
        this.addInputSchema((String)conf.get(StreamingConfig.STREAMING_INNER_INPUT_STREAM_NAME),tupleEventType);
        
        if (conf.containsKey(StreamingConfig.STREAMING_INNER_INPUT_SCHEMA))
        {
            if (null != serde)
            {
                serde.setSchema(tupleEventType);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void initialize()
     throws StreamingException
    {
        initializeSerDe();
        outputStream.setSerDe(getSerDe());
        outputStream.initialize();
    }

    /**
     * 获取序列化类
     */
    public StreamSerDe getSerDe()
    {
        return serde;
    }
    
    /**
     * 设置序列化类
     */
    public void setSerDe(StreamSerDe serde)
    {
        this.serde = serde;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final void setOutputStream(String streamNames)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final String getOutputStream()
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final void setOutputSchema(IEventType schemas)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final IEventType getOutputSchema()
    {
        return null;
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
        return inputSchemas;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        outputStream.execute(streamName, event);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() throws StreamingException
    {
        outputStream.destroy();
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

    private void initializeSerDe() throws StreamingException
    {
        if(getSerDe() == null)
        {
            StreamingException exception = new StreamingException(ErrorCode.SEMANTICANALYZE_UNKNOWN_SERDE);
            LOG.error(ErrorCode.SEMANTICANALYZE_UNKNOWN_SERDE.getFullMessage());
            throw exception;
        }

        try
        {
            getSerDe().initialize();
        }
        catch (StreamSerDeException e)
        {
            throw StreamingException.wrapException(e);
        }
    }
}
