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
import com.huawei.streaming.cql.semanticanalyzer.OrderByClauseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.Task;

/**
 * orderby子句语法分析内容
 * 
 */
public class OrderbyClauseContext extends ParseContext
{
    private List<ColumnNameOrderContext> orderColumns;
    
    /**
     * <默认构造函数>
     */
    public OrderbyClauseContext()
    {
        orderColumns = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "ORDER BY " + Joiner.on(", ").join(orderColumns);
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
        return new OrderByClauseAnalyzer(this);
    }
    
    public List<ColumnNameOrderContext> getOrderColumns()
    {
        return orderColumns;
    }
    
    public void setOrderColumns(List<ColumnNameOrderContext> orderColumns)
    {
        this.orderColumns = orderColumns;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        for (ColumnNameOrderContext col : orderColumns)
        {
            walkExpression(walker, col);
        }
    }
    
}
