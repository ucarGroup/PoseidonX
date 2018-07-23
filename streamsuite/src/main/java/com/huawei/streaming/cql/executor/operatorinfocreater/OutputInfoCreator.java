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

package com.huawei.streaming.cql.executor.operatorinfocreater;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OutputStreamOperator;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.AbsOperator;
import com.huawei.streaming.operator.IOutputStreamOperator;
import com.huawei.streaming.operator.OutputOperator;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * 创建输出算子信息
 *
 */
public class OutputInfoCreator implements OperatorInfoCreator
{
    
    private static final Logger LOG = LoggerFactory.getLogger(OutputInfoCreator.class);
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AbsOperator createInstance(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemConfig)
        throws StreamingException
    {
        OutputStreamOperator op = (OutputStreamOperator)operator;

        //参数顺序应该是默认参数--> 全局参数--》局部参数
        StreamingConfig config = new StreamingConfig();
        config.putAll(systemConfig);
        if (operator.getArgs() != null)
        {
            config.putAll(operator.getArgs());
        }

        
        StreamSerDe ser = createSerializerInstance(op, config);
        IOutputStreamOperator fop = createRecordWriterInstance(op.getRecordWriterClassName());
         fop.setConfig(config);

        OutputOperator outputOperator = new OutputOperator();
        outputOperator.setSerDe(ser);
        outputOperator.setOutputStreamOperator(fop);

        return OperatorInfoCreatorFactory.buildStreamOperator(operator, outputOperator);
    }
    
    /**
     * 创建序列化类实例
     *
     */
    private StreamSerDe createSerializerInstance(OutputStreamOperator op, StreamingConfig config)
        throws ExecutorException
    {
        String serializerClassName = op.getSerializerClassName();
        if (serializerClassName == null)
        {
            return null;
        }
        StreamSerDe ser = createSerDeInstance(op, serializerClassName);
        initSerDe(op, config, ser);
        return ser;
    }

    private void initSerDe(OutputStreamOperator op, StreamingConfig config, StreamSerDe ser)
        throws ExecutorException
    {
        try
        {
            ser.setConfig(config);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    private StreamSerDe createSerDeInstance(OutputStreamOperator op, String serializerClassName)
        throws ExecutorException
    {
        StreamSerDe ser = null;
        try
        {
            ser = (StreamSerDe)Class.forName(serializerClassName, true, CQLUtils.getClassLoader()).newInstance();
        }
        catch (ReflectiveOperationException e)
        {
            ExecutorException exception =
                new ExecutorException(e, ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, op.getSerializerClassName());
            LOG.error("Failed to create SerDe instance.", exception);
            throw exception;
        }
        return ser;
    }
    
    /**
     * 创建recordwriter实例
     *
     */
    private IOutputStreamOperator createRecordWriterInstance(String sinkClass)
        throws ExecutorException
    {
        Object operator = null;
        try
        {
            operator = Class.forName(sinkClass, true, CQLUtils.getClassLoader()).newInstance();
        }
        catch (ReflectiveOperationException e)
        {
            ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, sinkClass);
            LOG.error("Can't find record writer class.", exception);
            throw exception;
        }

        if (operator instanceof IOutputStreamOperator)
        {
            return (IOutputStreamOperator)operator;
        }
        else
        {
            ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_UNMATCH_OPERATOR, sinkClass);
            LOG.error("The '{}' operator type does not match.", sinkClass);
            throw exception;
        }
    }
    
}
