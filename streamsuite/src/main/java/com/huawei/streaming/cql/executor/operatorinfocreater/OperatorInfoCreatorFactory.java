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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.AnnotationUtils;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.AbsOperator;

/**
 * 创建算子信息实例的工厂类
 *
 */
public class OperatorInfoCreatorFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(OperatorInfoCreatorFactory.class);
    
    /**
     * 创建各类应用程序中待提交的算子信息实例
     *
     */
    public static AbsOperator createOperatorInfo(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemconfig)
        throws ExecutorException
    {
        OperatorInfoCreator creator = createOperatorInfoInstance(operator);
        try
        {
            return creator.createInstance(vapp, operator, streamschema, systemconfig);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    private static OperatorInfoCreator createOperatorInfoInstance(Operator operator)
        throws ExecutorException
    {
        Class< ? extends OperatorInfoCreator> creatorClass =
            AnnotationUtils.getOperatorCreatorAnnotation(operator.getClass());
        if (creatorClass == null)
        {
            ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS);
            LOG.error("Unkown operator class.", exception);
            throw exception;
        }
        
        try
        {
            return creatorClass.newInstance();
        }
        catch (ReflectiveOperationException e)
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, operator.getClass().getName());
            LOG.error("Failed to create operator class instance.", exception);
            
            throw exception;
        }
    }
    
    /**
     * 获取进入算子的连线
     * <p/>
     * 在一个算子中，只能接口相同流名称的一个连接
     *
     */
    protected static List<OperatorTransition> getTransitionIn(Application vapp, Operator operator)
        throws ExecutorException
    {
        List<OperatorTransition> result = new ArrayList<OperatorTransition>();
        
        for (OperatorTransition transition : vapp.getOpTransition())
        {
            if (transition.getToOperatorId().equals(operator.getId()))
            {
                result.add(transition);
            }
        }
        
        if (result.size() == 0)
        {
            ExecutorException exception = new ExecutorException(ErrorCode.TOP_TRANSITION_TO, operator.getId());
            LOG.error("Invalid topology.", exception);
            throw exception;
        }
        
        return result;
    }
    
    /**
     * 获取进入算子的连线
     *
     */
    protected static OperatorTransition getTransitionIn(Application vapp, Operator operator, String streamName)
        throws ExecutorException
    {
        for (OperatorTransition transition : vapp.getOpTransition())
        {
            if (StringUtils.isEmpty(streamName))
            {
                if (transition.getToOperatorId().equals(operator.getId()))
                {
                    return transition;
                }
            }
            else
            {
                if (transition.getStreamName().equals(streamName)
                    && transition.getToOperatorId().equals(operator.getId()))
                {
                    return transition;
                }
            }
            
        }
        ExecutorException exception = new ExecutorException(ErrorCode.TOP_TRANSITION_TO, operator.getId());
        LOG.error("Can't find input stream for operator.", exception);
        throw exception;
    }
    
    /**
     * 获取离开该算子的连线
     *
     */
    protected static OperatorTransition getTransitionOut(Application vapp, Operator operator, String streamName)
        throws ExecutorException
    {
        for (OperatorTransition transition : vapp.getOpTransition())
        {
            if (transition.getFromOperatorId().equals(operator.getId()))
            {
                if (!StringUtils.isEmpty(streamName))
                {
                    if (transition.getStreamName().equals(streamName))
                    {
                        return transition;
                    }
                }
                else
                {
                    return transition;
                }
            }
        }
        ExecutorException exception = new ExecutorException(ErrorCode.TOP_TRANSITION_FROM, operator.getId());
        LOG.error("Can not find output stream for operator.", exception);
        throw exception;
    }
    
    /**
     * 获取离开该算子的连线
     *
     */
    protected static OperatorTransition getTransitionOut(Application vapp, Operator operator)
        throws ExecutorException
    {
        for (OperatorTransition transition : vapp.getOpTransition())
        {
            if (transition.getFromOperatorId().equals(operator.getId()))
            {
                return transition;
            }
        }
        
        ExecutorException exception = new ExecutorException(ErrorCode.TOP_TRANSITION_FROM, operator.getId());
        LOG.error("Can't find output stream from operator.", exception);
        throw exception;
    }
    
    /**
     * 获取算子中用到的schema信息
     * <p/>
     * 需要根据连线获取
     * 1、获取所有向该聚合算子发起连接的连线
     * 2、获取该聚合算子的schema
     *
     */
    protected static List<Schema> getSchemasByTransition(Application vapp, OperatorTransition transition)
        throws ExecutorException
    {
        List<Schema> schemas = Lists.newArrayList();
        Schema schema = getClonedSchemaByName(transition.getSchemaName(), vapp);
        schema.setStreamName(transition.getStreamName());
        schemas.add(schema);
        return schemas;
    }
    
    /**
     * 通过schema名称获取schema
     *
     */
    public static Schema getClonedSchemaByName(String schemaName, Application vapp)
        throws ExecutorException
    {
        for (Schema schema : vapp.getSchemas())
        {
            if (schema.getId().equalsIgnoreCase(schemaName))
            {
                return schema.cloneSchema();
            }
        }
        ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_NOFOUND_STREAM, schemaName);
        LOG.error("Can't find stream schema.", exception);
        throw exception;
    }
    
    /**
     * 创建一个流处理算子
     *
     */
    protected static AbsOperator buildStreamOperator(Operator operator, AbsOperator streamOperator)
        throws ExecutorException
    {
        streamOperator.setParallelNumber(operator.getParallelNumber());
        streamOperator.setOperatorId(operator.getId());
        return streamOperator;
    }
}
