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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.FunctionStreamOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.AbsOperator;
import com.huawei.streaming.operator.IFunctionStreamOperator;

/**
 * 创建最一般的自定义算子
 * <p/>
 * 在创建自定义算子的时候，要根据连线，来确定属于第一个算子还是最后一个算子
 * 以此来决定是inputStream或者OutputStream
 *
 */
public class FunctionStreamInfoCreator implements OperatorInfoCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionStreamInfoCreator.class);
    
    private FunctionStreamOperator streamOperator;
    
    private OperatorTransition transitionIn = null;
    
    private OperatorTransition transitionOut = null;
    
    private List<Schema> inputSchemas;
    
    private List<Schema> outputSchemas;
    
    /**
     * 创建自定义算子
     *
     */
    @Override
    public AbsOperator createInstance(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemConfig)
        throws StreamingException
    {
        prepare(vapp, operator);
        checkSchema();
        
        IFunctionStreamOperator opInstance = createOperatorInstance();
        //这里可以放心的setConfig，BasicStreamOperator创建完成之后，就不会再次new任何StreamingConfig对象了
        setConfig(operator, systemConfig, opInstance);
        
        com.huawei.streaming.operator.FunctionStreamOperator functionStreamOperator =
            new com.huawei.streaming.operator.FunctionStreamOperator();
        
        functionStreamOperator.setInputStreamOperator(opInstance);
        
        return (AbsOperator)OperatorInfoCreatorFactory.buildStreamOperator(operator, functionStreamOperator);
    }
    
    private void checkSchema()
        throws StreamingException
    {
        validateSchemaSize();
        validateColumnType();
    }
    
    private void validateColumnType()
        throws StreamingException
    {
        if (!checkSchemaColumnType(inputSchemas.get(0), streamOperator.getInputSchema()))
        {
            StreamingException exception = new StreamingException(ErrorCode.SEMANTICANALYZE_INVALID_INPUTSCHEMA);
            LOG.error("Invalid input schema in user defined operator.");
            throw exception;
        }
        
        if (!checkSchemaColumnType(outputSchemas.get(0), streamOperator.getOutputSchema()))
        {
            StreamingException exception = new StreamingException(ErrorCode.SEMANTICANALYZE_INVALID_OUTPUTSCHEMA);
            LOG.error("Invalid output schema in user defined operator.");
            throw exception;
        }
    }
    
    private void validateSchemaSize()
        throws StreamingException
    {
        if (inputSchemas.size() != 1)
        {
            StreamingException exception = new StreamingException(ErrorCode.SEMANTICANALYZE_OVER_INPUTSCHEMA);
            LOG.error("Only one input schema is allowed in user defined operator.");
            throw exception;
        }
        
        if (outputSchemas.size() != 1)
        {
            StreamingException exception = new StreamingException(ErrorCode.SEMANTICANALYZE_OVER_OUTPUTSCHEMA);
            LOG.error("Only one output schema is allowed in user defined operator.");
            throw exception;
        }
    }
    
    private void prepare(Application vapp, Operator operator)
        throws ExecutorException
    {
        streamOperator = (FunctionStreamOperator)operator;
        this.transitionIn = OperatorInfoCreatorFactory.getTransitionIn(vapp, operator, null);
        this.transitionOut = OperatorInfoCreatorFactory.getTransitionOut(vapp, operator);
        this.inputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, transitionIn);
        this.outputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, transitionOut);
    }
    
    private void setConfig(Operator operator, Map<String, String> systemConfig, IFunctionStreamOperator opInstance)
        throws StreamingException
    {
        StreamingConfig config = new StreamingConfig();
        config.putAll(systemConfig);
        if (operator.getArgs() != null)
        {
            config.putAll(operator.getArgs());
        }
        opInstance.setConfig(config);
    }
    
    private IFunctionStreamOperator createOperatorInstance()
        throws ExecutorException
    {
        String operatorClass = streamOperator.getOperatorClass();
        try
        {
            return (IFunctionStreamOperator)Class.forName(operatorClass, true, CQLUtils.getClassLoader()).newInstance();
        }
        catch (ReflectiveOperationException e)
        {
            ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, operatorClass);
            LOG.error("Failed to create operator instance.", exception);
            
            throw exception;
        }
    }
    
    private boolean checkSchemaColumnType(Schema schema1, Schema schema2)
    {
        if (schema1 == null || schema2 == null)
        {
            return false;
        }
        
        List<Column> schema1Columns = schema1.getCols();
        List<Column> schema2Columns = schema2.getCols();
        
        if (schema1Columns.size() != schema2Columns.size())
        {
            return false;
        }
        
        for (int i = 0; i < schema1Columns.size(); i++)
        {
            if (!schema1Columns.get(i).getType().equals(schema2Columns.get(i).getType()))
            {
                return false;
            }
        }
        
        return true;
    }
    
}
