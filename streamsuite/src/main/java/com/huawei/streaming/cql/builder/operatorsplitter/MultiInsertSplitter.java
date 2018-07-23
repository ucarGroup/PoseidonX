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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.SplitterOperator;
import com.huawei.streaming.api.opereators.SplitterSubContext;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.BaseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FilterClauseAnalzyeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FromClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertStatementAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.ParallelClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;

/**
 * 多级insert算子拆分
 *
 */
public class MultiInsertSplitter implements Splitter
{
    private static final Logger LOG = LoggerFactory.getLogger(MultiInsertSplitter.class);
    
    private BuilderUtils bUtils;
    
    private MultiInsertStatementAnalyzeContext context;
    
    private SplitContext result = new SplitContext();
    
    private int parallelNumber = 1;
    
    /**
     * <默认构造函数>
     *
     */
    public MultiInsertSplitter(BuilderUtils buildUtils)
    {
        bUtils = buildUtils;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(AnalyzeContext parseContext)
    {
        return parseContext instanceof MultiInsertStatementAnalyzeContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SplitContext split(AnalyzeContext parseContext)
        throws ApplicationBuildException
    {
        LOG.debug("start to split multiInsert analyze context.");
        context = (MultiInsertStatementAnalyzeContext)parseContext;
        setParallelNumber();
        String inputStreamName = context.getFrom().getInputStreams().get(0);
        FilterOperator fop = splitFiterBeforeWindow(inputStreamName);
        SplitterOperator splitter = new SplitterOperator(bUtils.getNextOperatorName("Splitter"), parallelNumber);
        parseSubSelect(splitter);
        Schema schema = context.getFrom().getInputSchemas().get(0);
        OperatorTransition transition =
            new OperatorTransition(bUtils.getNextStreamName(), fop, splitter, DistributeType.SHUFFLE, null, schema);
        
        result.getOperators().add(fop);
        result.getOperators().add(splitter);
        result.setParseContext(context);
        result.getTransitions().add(transition);
        return result;
    }
    
    public ParallelClauseAnalyzeContext getParallelClauseContext()
    {
        return context.getParallelClause();
    }
    
    /**
     * filter before window 语句解析
     *
     */
    protected FilterOperator splitFiterBeforeWindow(String streamName)
        throws SemanticAnalyzerException
    {
        FilterOperator fop = new FilterOperator(bUtils.getNextOperatorName("Filter"), parallelNumber);
        fop.setFilterExpression(getFilterExpression(streamName));
        fop.setOutputExpression(createFilterOutputExpression(streamName));
        return fop;
    }
    
    private String getFilterExpression(String streamName)
    {
        FromClauseAnalyzeContext clauseContext = context.getFrom();
        ExpressionDescribe expression = clauseContext.getFilterBeForeWindow().get(streamName);
        if (expression == null)
        {
            return null;
        }
        return expression.toString();
    }
    
    private void parseSubSelect(SplitterOperator splitter)
    {
        //必须保证按照顺序循环，因为在combiner中还要使用
        for (int i = 0; i < context.getMultiSelectBodyAnalyzeContexts().size(); i++)
        {
            MultiInsertAnalyzeContext insert = context.getMultiSelectBodyAnalyzeContexts().get(i);
            SplitterSubContext subContext = new SplitterSubContext();
            setFilterExpression(insert, subContext);
            subContext.setOutputExpression(insert.getSelectContext().getSelectClauseContext().toString());
            splitter.getSubSplitters().add(subContext);
        }
    }
    
    private String createFilterOutputExpression(String streamName)
        throws SemanticAnalyzerException
    {
        FromClauseAnalyzeContext clauseContext = context.getFrom();
        Schema schema = BaseAnalyzer.getSchemaByName(streamName, clauseContext.getInputSchemas());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schema.getCols().size(); i++)
        {
            sb.append(schema.getId() + "." + schema.getCols().get(i).getName());
            if (i != schema.getCols().size() - 1)
            {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    private void setFilterExpression(MultiInsertAnalyzeContext insert, SplitterSubContext subContext)
    {
        FilterClauseAnalzyeContext where = insert.getSelectContext().getWhereClauseContext();
        if (where != null)
        {
            subContext.setFilterExpression(where.toString());
        }
    }
    
    private void setParallelNumber()
    {
        if (getParallelClauseContext() == null || getParallelClauseContext().getParallelNumber() == null)
        {
            parallelNumber = bUtils.getDefaultParallelNumber();
        }
        else
        {
            parallelNumber = getParallelClauseContext().getParallelNumber();
        }
    }
}
