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

package com.huawei.streaming.cql.semanticanalyzer.parser.context;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.SelectClauseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.Task;

/**
 * group by子句语法解析内容
 * 
 */
public class GroupbyClauseContext extends ParseContext
{
    private List<ExpressionContext> expressions;
    
    /**
     * <默认构造函数>
     */
    public GroupbyClauseContext()
    {
        expressions = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "GROUP BY " + Joiner.on(", ").join(expressions);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException
    {
        SelectClauseContext selectContext = new SelectClauseContext();
        for (ExpressionContext exp : expressions)
        {
            SelectExpressionContext selectExp = new SelectExpressionContext();
            selectExp.setExpression(exp);
            
            SelectItemContext item = new SelectItemContext();
            item.setExpression(selectExp);
            selectContext.getSelectItems().add(item);
        }
        return new SelectClauseAnalyzer(selectContext);
    }
    
    public List<ExpressionContext> getExpressions()
    {
        return expressions;
    }
    
    public void setExpressions(List<ExpressionContext> expressions)
    {
        this.expressions = expressions;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        for (ExpressionContext exp : expressions)
        {
            walkExpression(walker, exp);
        }
        
    }
}
