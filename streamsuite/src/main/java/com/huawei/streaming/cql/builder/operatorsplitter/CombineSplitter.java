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

package com.huawei.streaming.cql.builder.operatorsplitter;

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.opereators.CombineOperator;
import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.BaseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FromClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.StreamAliasDesc;
import com.huawei.streaming.exception.ErrorCode;

/**
 * combine语句拆分
 *
 */
public class CombineSplitter extends SelectSplitter
{
    private static final Logger LOG = LoggerFactory.getLogger(CombineSplitter.class);
    
    /**
     * <默认构造函数>
     *
     */
    public CombineSplitter(BuilderUtils buildUtils)
    {
        super(buildUtils);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(AnalyzeContext parseContext)
    {
        if (!(parseContext instanceof SelectAnalyzeContext))
        {
            return false;
        }
        
        SelectAnalyzeContext selectAnalyzeContext = (SelectAnalyzeContext)parseContext;
        FromClauseAnalyzeContext clauseContext = selectAnalyzeContext.getFromClauseContext();
        return clauseContext.getCombineConditions().size() != 0;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void splitFromClause()
        throws ApplicationBuildException
    {
        String opName = getBuildUtils().getNextOperatorName("Combiner");
        CombineOperator combineoperator = new CombineOperator(opName, getParallelNumber());
        combineoperator.setOutputExpression(getSelectClauseContext().toString());
        combineoperator.setCombineProperties(createCombineConditions());
        combineoperator.setOrderedStreams(createOrderedStreams());
        
        TreeMap<String, FilterOperator> filters = createFilterOperators();
        List<OperatorTransition> transitions = createCombineTransitions(filters, combineoperator);
        for (Entry<String, FilterOperator> et : filters.entrySet())
        {
            getResult().addOperators(et.getValue());
        }
        getResult().addOperators(combineoperator);
        getResult().getTransitions().addAll(transitions);
        
    }
    
    private String createCombineConditions()
        throws ApplicationBuildException
    {
        List<StreamAliasDesc> streams = getCombineStreams();
        TreeMap<String, PropertyValueExpressionDesc> conditions = getFromClauseContext().getCombineConditions();
        
        checkCombineCondition(streams, conditions);
        
        //TODO combine中别名的支持
        return expressionsToString(conditions);
    }
    
    private void checkCombineCondition(List<StreamAliasDesc> streams,
     TreeMap<String, PropertyValueExpressionDesc> conditions)
        throws ApplicationBuildException
    {
        if (streams.size() != conditions.size())
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_COMBINE_SIZE, String.valueOf(streams.size()),
                    String.valueOf(conditions.size()));
            LOG.error("Stream size not match condition size.", exception);
            throw exception;
        }
    }
    
    private String createOrderedStreams()
    {
        List<StreamAliasDesc> list = getCombineStreams();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++)
        {
            sb.append(list.get(i).getStreamName() + ",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
    
    private List<OperatorTransition> createCombineTransitions(TreeMap<String, FilterOperator> filters,
        CombineOperator combineOperator)
        throws SemanticAnalyzerException
    {
        List<OperatorTransition> results = Lists.newArrayList();
        for (Entry<String, FilterOperator> et : filters.entrySet())
        {
            String streamName = et.getKey();
            FilterOperator fromop = et.getValue();
            OperatorTransition t = createCombineTransition(fromop, combineOperator, streamName);
            results.add(t);
        }
        return results;
    }
    
    private TreeMap<String, FilterOperator> createFilterOperators()
        throws SemanticAnalyzerException
    {
        List<StreamAliasDesc> streams = getCombineStreams();
        TreeMap<String, FilterOperator> filters = Maps.newTreeMap();
        
        for (StreamAliasDesc sad : streams)
        {
            String streamName = sad.getStreamName();
            FilterOperator fad = splitFiterBeforeWindow(streamName);
            filters.put(streamName, fad);
        }
        
        return filters;
    }
    
    private String expressionsToString(TreeMap<String, PropertyValueExpressionDesc> conditions)
    {
        StringBuilder sb = new StringBuilder();
        for (Entry<String, PropertyValueExpressionDesc> et : conditions.entrySet())
        {
            PropertyValueExpressionDesc pvd = et.getValue();
            sb.append(pvd.toString() + ",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
    
    private OperatorTransition createCombineTransition(Operator fromOp, Operator toOp, String streamName)
        throws SemanticAnalyzerException
    {
        TreeMap<String, PropertyValueExpressionDesc> conditions = getFromClauseContext().getCombineConditions();
        
        List<Schema> inputSchemas = getFromClauseContext().getInputSchemas();
        String disFields = conditions.get(streamName).toString();
        Schema schema = BaseAnalyzer.getSchemaByName(streamName, inputSchemas);
        return new OperatorTransition(getBuildUtils().getNextStreamName(), fromOp, toOp, DistributeType.FIELDS,
            disFields, schema);
    }
    
    private List<StreamAliasDesc> getCombineStreams()
    {
        return getFromClauseContext().getJoinexpression().getOrderedStreams();
    }
    
}
