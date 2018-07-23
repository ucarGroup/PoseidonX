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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.AggregateOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.operatorviewscreater.AggResultSetMergeViewCreator;
import com.huawei.streaming.cql.executor.operatorviewscreater.AggResultSetParameters;
import com.huawei.streaming.cql.executor.operatorviewscreater.FilterViewExpressionCreator;
import com.huawei.streaming.cql.executor.operatorviewscreater.WindowViewCreator;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.operator.AbsOperator;
import com.huawei.streaming.operator.functionstream.AggFunctionOp;
import com.huawei.streaming.process.agg.resultmerge.IAggResultSetMerge;
import com.huawei.streaming.view.FilterView;
import com.huawei.streaming.window.IWindow;

/**
 * 创建聚合算子信息
 * 
 */
public class AggregaterInfoCreator implements OperatorInfoCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(AggregaterInfoCreator.class);
    
    private AggregateOperator aggOperator;
    
    private OperatorTransition transitionIn = null;
    
    private OperatorTransition transitionOut = null;
    
    private List<Schema> inputSchemas;
    
    private List<Schema> outputSchemas;
    
    private Map<String, String> applicationConfig;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AbsOperator createInstance(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemConfig)
        throws StreamingException
    {
        LOG.debug("start to create aggregate operator");
        prepare(vapp, operator, systemConfig);
        WindowViewCreator creater = new WindowViewCreator();

        Window aggOperatorWindow = aggOperator.getWindow();

        aggOperatorWindow.setGroupbyExpression(aggOperator.getGroupbyExpression());
        aggOperatorWindow.setOutputExpression(aggOperator.getOutputExpression());

        IWindow window = creater.create(inputSchemas, aggOperatorWindow, this.applicationConfig);

        FilterView filterView = createFilterView();
        IExpression bexpr = null;
        if (filterView != null)
        {
            bexpr = filterView.getBoolexpr();
            
        }
        AggResultSetParameters pars = createResultSetMergeParmeters(streamschema, window, bexpr);
        IAggResultSetMerge resultSetMerge = new AggResultSetMergeViewCreator(pars).create();
        AggFunctionOp aggFunctionOp =
            new AggFunctionOp(window, filterView, resultSetMerge,
                OutputTypeAnalyzer.createOutputType(aggOperator.getWindow()));
        
        StreamingConfig config = new StreamingConfig();
        if (operator.getArgs() != null)
        {
            config.putAll(operator.getArgs());
        }
        config.putAll(this.applicationConfig);
        aggFunctionOp.setConfig(config);
        
        return OperatorInfoCreatorFactory.buildStreamOperator(operator, aggFunctionOp);
    }
    
    private void prepare(Application vapp, Operator operator, Map<String, String> systemconfig)
        throws ExecutorException
    {
        this.applicationConfig = systemconfig;
        this.aggOperator = (AggregateOperator)operator;
        this.transitionIn = OperatorInfoCreatorFactory.getTransitionIn(vapp, operator, null);
        this.transitionOut = OperatorInfoCreatorFactory.getTransitionOut(vapp, operator);
        this.inputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, transitionIn);
        this.outputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, transitionOut);
    }
    
    private AggResultSetParameters createResultSetMergeParmeters(EventTypeMng streamschema, IWindow window,
        IExpression whereExpression)
        throws ExecutorException
    {
        Map<String, IWindow> streamWindows = Maps.newHashMap();
        streamWindows.put(inputSchemas.get(0).getStreamName(), window);
        
        List<Window> operatorWindows = Lists.newArrayList();
        operatorWindows.add(aggOperator.getWindow());
        
        AggResultSetParameters pars = new AggResultSetParameters();
        pars.setBasicAggOperator(aggOperator);
        pars.setInputSchemas(inputSchemas);
        pars.setStreamschema(streamschema);
        pars.setOutputSchemas(outputSchemas);
        pars.setTransitionOut(transitionOut);
        pars.setStreamWindows(streamWindows);
        pars.setExpressionBeforeAggregate(whereExpression);
        pars.setSystemConfig(applicationConfig);
        pars.setOperatorWindows(operatorWindows);
        return pars;
    }
    
    private FilterView createFilterView()
        throws ExecutorException
    {
        String whereExpression = aggOperator.getFilterBeforeAggregate();
        final FilterViewExpressionCreator creator = new FilterViewExpressionCreator();
        IExpression filterExpression = creator.create(inputSchemas, whereExpression, applicationConfig);
        
        if (null == filterExpression)
        {
            return null;
        }
        
        return new FilterView(filterExpression);
    }
    
}
