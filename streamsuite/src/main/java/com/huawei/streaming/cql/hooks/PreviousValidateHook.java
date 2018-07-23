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

package com.huawei.streaming.cql.hooks;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.PreviousExpressionWalker;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.InsertStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectItemContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectStatementContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * previous函数检查
 * 
 * 1、previous函数可以放在任何地方，select子句、where、having以及聚合表达式中。
 * 2、select子句中，如果previous中包含了多列，是不允许设置别名的
 * 3、where和having子句以及aggregate表达式中，不允许出现包含多列的
 * 
 */
public class PreviousValidateHook implements SemanticAnalyzeHook
{
    private static final Logger LOG = LoggerFactory.getLogger(PreviousValidateHook.class);
    
    private PreviousExpressionWalker walker = null;
    
    /**
     * <默认构造函数>
     */
    public PreviousValidateHook()
    {
        walker = new PreviousExpressionWalker();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        return (parseContext instanceof InsertStatementContext) || (parseContext instanceof SelectStatementContext);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void preAnalyze(DriverContext context, ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        LOG.debug("start to execute previous preanalyze hook");
        SelectStatementContext selectContext = getSelectContext(parseContext);
        if (selectContext == null)
        {
            return;
        }
        walkExpressions(selectContext);
        validatePrivous();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void postAnalyze(DriverContext context, AnalyzeContext analyzeConext)
        throws SemanticAnalyzerException
    {
        
    }
    
    private void validatePrivous()
        throws SemanticAnalyzerException
    {
        if (walker.isHasMultiPreviousExpression())
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.FUNCTION_PREVIOUS_PROPERTYVALUE_EXPRSSION);
            LOG.error("Previous syntax error.", exception);
            
            throw exception;
        }
    }
    
    private void walkExpressions(SelectStatementContext selectContext)
    {
        List<ExpressionContext> expressions = getExpressionContexts(selectContext);
        for (ExpressionContext exp : expressions)
        {
            exp.walk(walker);
        }
    }
    
    private List<ExpressionContext> getExpressionContexts(SelectStatementContext select)
    {
        List<ExpressionContext> exps = Lists.newArrayList();
        getExpressionsFromSelectClause(select, exps);
        getExpressionsFromWhereClause(select, exps);
        getExpressionsFromHavingClause(select, exps);
        return exps;
    }
    
    private void getExpressionsFromHavingClause(SelectStatementContext select, List<ExpressionContext> exps)
    {
        if (select.getHaving() != null)
        {
            exps.add(select.getHaving().getExpression());
        }
    }
    
    private void getExpressionsFromWhereClause(SelectStatementContext select, List<ExpressionContext> exps)
    {
        if (select.getWhere() != null)
        {
            exps.add(select.getWhere().getExpression());
        }
    }
    
    private void getExpressionsFromSelectClause(SelectStatementContext select, List<ExpressionContext> exps)
    {
        List<SelectItemContext> selectItems = select.getSelect().getSelectItems();
        for (SelectItemContext item : selectItems)
        {
            ExpressionContext exp = item.getExpression().getExpression();
            if (exp != null)
            {
                exps.add(exp);
            }
        }
    }
    
    private SelectStatementContext getSelectContext(ParseContext parseContext)
    {
        SelectStatementContext selectContext = null;
        if (parseContext instanceof InsertStatementContext)
        {
            InsertStatementContext icontext = (InsertStatementContext)parseContext;
            selectContext = icontext.getSelect();
        }
        if (parseContext instanceof SelectStatementContext)
        {
            selectContext = (SelectStatementContext)parseContext;
        }
        return selectContext;
    }
    
}
