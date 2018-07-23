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

import com.google.common.collect.Maps;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.SplitterOperator;
import com.huawei.streaming.api.opereators.SplitterSubContext;
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
import com.huawei.streaming.operator.functionstream.SplitOp;
import com.huawei.streaming.process.SelectSubProcess;

/**
 * Spliter算子实例创建
 *
 */
public class SplitterInfoCreator implements OperatorInfoCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(SplitterInfoCreator.class);
    
    private SplitterOperator splitterOperator;
    
    private OperatorTransition transitionIn = null;
    
    private List<Schema> inputSchemas;
    
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
        
        Map<String, SelectSubProcess> selector = Maps.newHashMap();
        Map<String, IExpression> filters = Maps.newHashMap();
        Map<String, IEventType> schemas = Maps.newHashMap();
        
        for (SplitterSubContext splitterSubContext : splitterOperator.getSubSplitters())
        {
            String streamName = splitterSubContext.getStreamName();
            String filterExpression = splitterSubContext.getFilterExpression();
            String outputExpression = splitterSubContext.getOutputExpression();
            
            IExpression filterExp =
                new FilterViewExpressionCreator().create(inputSchemas, filterExpression, systemConfig);
            
            OperatorTransition transitionOut = OperatorInfoCreatorFactory.getTransitionOut(vapp, operator, streamName);
            IEventType outputTupleEvent = streamschema.getEventType(transitionOut.getSchemaName());
            filters.put(streamName, filterExp);
            selector.put(streamName, createSelectProcess(outputExpression, outputTupleEvent));
            schemas.put(streamName, outputTupleEvent);
        }
        
        SplitOp sop = new SplitOp(selector, filters, schemas);
        
        StreamingConfig config = new StreamingConfig();
        config.putAll(this.applicationConfig);
        if (operator.getArgs() != null)
        {
            config.putAll(operator.getArgs());
        }
        
        sop.setConfig(config);
        return OperatorInfoCreatorFactory.buildStreamOperator(operator, sop);
    }
    
    private void prepare(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemconfig)
        throws ExecutorException
    {
        this.applicationConfig = systemconfig;
        this.splitterOperator = (SplitterOperator)operator;
        this.transitionIn = OperatorInfoCreatorFactory.getTransitionIn(vapp, operator, null);
        this.inputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, transitionIn);
    }
    
    private SelectSubProcess createSelectProcess(String outputExpression, IEventType outputTupleEvent)
        throws ExecutorException
    {
        SelectViewExpressionCreator creater = new SelectViewExpressionCreator();
        IExpression[] exprs = creater.create(inputSchemas, outputExpression, applicationConfig);
        
        if (exprs.length != outputTupleEvent.getAllAttributes().length)
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_NOTSAME_COLUMNS, String.valueOf(exprs.length),
                    String.valueOf(outputTupleEvent.getAllAttributes().length));
            LOG.error("Select column not match output tuple column.", exception);
            throw exception;
        }
        return new SelectSubProcess(transitionIn.getStreamName(), exprs, null, outputTupleEvent);
    }
}
