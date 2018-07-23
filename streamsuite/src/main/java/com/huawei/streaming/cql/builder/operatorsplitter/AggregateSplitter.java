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

import com.huawei.streaming.api.opereators.AggregateOperator;
import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FromClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;

/**
 * 聚合类型的select语句拆分
 * 
 */
public class AggregateSplitter extends SelectSplitter
{
    
    /**
     * <默认构造函数>
     */
    public AggregateSplitter(BuilderUtils buildUtils)
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
        
        //属于select,然后既不是combine，又不是join，那么就是aggregate
        if (clauseContext.getJoinexpression() != null)
        {
            return false;
        }
        
        if (clauseContext.getCombineConditions().size() != 0)
        {
            return false;
        }
        
        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void splitFromClause()
        throws ApplicationBuildException
    {
        FromClauseAnalyzeContext clauseContext = getFromClauseContext();
        
        String streamName = clauseContext.getInputStreams().get(0);
        FilterOperator fop = splitFiterBeforeWindow(streamName);
        AggregateOperator aggregateOperator = splitAggregateOperator(clauseContext, streamName);
        OperatorTransition transition = createTransition(fop, aggregateOperator, streamName);
        
        getResult().addOperators(fop);
        getResult().addOperators(aggregateOperator);
        getResult().addTransitions(transition);
    }
    
    private AggregateOperator splitAggregateOperator(FromClauseAnalyzeContext clauseContext, String streamName)
        throws ApplicationBuildException
    {
        AggregateOperator aggop = new AggregateOperator(getBuildUtils().getNextOperatorName("Aggregator"), getParallelNumber());
        
        parseWindow(clauseContext, streamName, aggop);
        parseWhere(aggop);
        
        aggop.setFilterAfterAggregate(parseHaving());
        aggop.setGroupbyExpression(parseGroupby());
        aggop.setOrderBy(parseOrderBy());
        aggop.setLimit(parseLimit());
        aggop.setOutputExpression(getSelectClauseContext().toString());
        
        return aggop;
    }
    
    private void parseWhere(AggregateOperator aggop)
    {
        if (getWhereClauseContext() != null)
        {
            aggop.setFilterBeforeAggregate(getWhereClauseContext().toString());
        }
    }
    
    private void parseWindow(FromClauseAnalyzeContext clauseContext, String streamName, AggregateOperator aggop)
    {
        Window win = clauseContext.getWindows().get(streamName);
        if (win != null)
        {
            aggop.setWindow(win);
        }
    }
    
}
