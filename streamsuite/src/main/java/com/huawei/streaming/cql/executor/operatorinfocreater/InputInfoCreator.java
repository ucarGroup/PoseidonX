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
import com.huawei.streaming.api.opereators.InputStreamOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.AbsOperator;
import com.huawei.streaming.operator.IInputStreamOperator;
import com.huawei.streaming.operator.InputOperator;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * 创建输入算子信息
 * <p/>
 * 由于申明映射的原因，这里传过来的operator的实例，一定是InputSourceOperator的
 * 所以直接强转
 *
 */
public class InputInfoCreator implements OperatorInfoCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(InputInfoCreator.class);
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AbsOperator createInstance(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemConfig)
        throws StreamingException
    {
        InputStreamOperator op = (InputStreamOperator)operator;

        //参数顺序应该是默认参数--> 全局参数--》局部参数
        StreamingConfig config = new StreamingConfig();
        config.putAll(systemConfig);
        if (operator.getArgs() != null)
        {
            config.putAll(operator.getArgs());
        }

        
        StreamSerDe des = createDeserClassInstance(op, config);
        IInputStreamOperator sourceOperator = createSourceReaderInstance(op.getRecordReaderClassName());
        sourceOperator.setConfig(config);     

        InputOperator inputOperator = new InputOperator();
        inputOperator.setSerDe(des);
        inputOperator.setInputStreamOperator(sourceOperator);

        return OperatorInfoCreatorFactory.buildStreamOperator(operator, inputOperator);
    }
    
    /**
     * 创建反序列化类实例
     *
     */
    private StreamSerDe createDeserClassInstance(InputStreamOperator op, StreamingConfig config)
        throws ExecutorException
    {
        String deserializerClassName = op.getDeserializerClassName();
        if (deserializerClassName == null)
        {
            return null;
        }
        
        StreamSerDe des = null;
        try
        {
            
            des = (StreamSerDe)Class.forName(deserializerClassName, true, CQLUtils.getClassLoader()).newInstance();
        }
        catch (ReflectiveOperationException e)
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, deserializerClassName);
            LOG.error("Failed to create Deser instance.", exception);
            throw exception;
        }

        try
        {
            des.setConfig(config);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
        return des;
    }
    
    /**
     * 创建sourceReader实例
     *
     */
    private IInputStreamOperator createSourceReaderInstance(String sourceClass)
        throws ExecutorException
    {
        Object operator = null;
        try
        {
            operator = Class.forName(sourceClass, true, CQLUtils.getClassLoader()).newInstance();
        }
        catch (Exception e)
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, sourceClass);
            LOG.error("Can't find source reader class.'", exception);
            throw exception;
        }

        if (operator instanceof IInputStreamOperator)
        {
            return (IInputStreamOperator)operator;
        }
        else
        {
            ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_UNMATCH_OPERATOR, sourceClass);
            LOG.error("The '{}' operator type does not match.", sourceClass);
            throw exception;
        }
    }
    
}
