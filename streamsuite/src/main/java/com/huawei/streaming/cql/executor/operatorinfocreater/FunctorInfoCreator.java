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
import com.huawei.streaming.api.opereators.FunctorOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.operatorviewscreater.FilterViewExpressionCreator;
import com.huawei.streaming.cql.executor.operatorviewscreater.SelectViewExpressionCreator;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.operator.AbsOperator;
import com.huawei.streaming.operator.functionstream.FunctorOp;
import com.huawei.streaming.process.SelectSubProcess;

/**
 * 创建功能性算子实例
 *
 */
public class FunctorInfoCreator implements OperatorInfoCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctorInfoCreator.class);
    
    private FunctorOperator functorOperator;
    
    private OperatorTransition transitionIn = null;
    
    private OperatorTransition transitionOut = null;
    
    private List<Schema> inputSchemas;
    
    private List<Schema> outputSchemas;
    
    private IEventType outputTupleEvent = null;
    
    private Map<String, String> applicationConfig;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AbsOperator createInstance(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemConfig)
        throws StreamingException
    {
        prepare(vapp, operator, streamschema, systemConfig);
        
        SelectSubProcess selectSubProcess = createSelectProcess();
        
        String strFilterExpression = functorOperator.getFilterExpression();
        
        IExpression filterExpression =
            new FilterViewExpressionCreator().create(inputSchemas, strFilterExpression, systemConfig);
        
        IEventType eventtype = streamschema.getEventType(transitionIn.getSchemaName());
        
        FunctorOp fop = new FunctorOp(selectSubProcess, eventtype, filterExpression);
        
        StreamingConfig config = new StreamingConfig();
        config.putAll(this.applicationConfig);
        if (operator.getArgs() != null)
        {
            config.putAll(operator.getArgs());
        }

        fop.setConfig(config);
        
        return OperatorInfoCreatorFactory.buildStreamOperator(operator, fop);
    }
    
    private void prepare(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemconfig)
        throws ExecutorException
    {
        this.applicationConfig = systemconfig;
        this.functorOperator = (FunctorOperator)operator;
        this.transitionIn = OperatorInfoCreatorFactory.getTransitionIn(vapp, operator, null);
        this.transitionOut = OperatorInfoCreatorFactory.getTransitionOut(vapp, operator);
        this.inputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, transitionIn);
        this.outputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, transitionOut);
        this.outputTupleEvent = streamschema.getEventType(transitionOut.getSchemaName());
    }
    
    private SelectSubProcess createSelectProcess()
        throws ExecutorException
    {
        String outputExpression = functorOperator.getOutputExpression();
        final SelectViewExpressionCreator creater = new SelectViewExpressionCreator();
        IExpression[] exprs = creater.create(inputSchemas, outputExpression, applicationConfig);
        if (exprs.length != outputSchemas.get(0).getCols().size())
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_NOTSAME_COLUMNS, String.valueOf(exprs.length),
                    String.valueOf(outputSchemas.get(0).getCols().size()));
            LOG.error("Select column not match output schema column.", exception);
            throw exception;
            
        }
        return new SelectSubProcess(transitionIn.getStreamName(), exprs, null, outputTupleEvent);
    }
}
