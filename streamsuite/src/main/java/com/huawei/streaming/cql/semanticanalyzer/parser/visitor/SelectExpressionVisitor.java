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

package com.huawei.streaming.cql.semanticanalyzer.parser.visitor;

import org.antlr.v4.runtime.misc.NotNull;

import com.huawei.streaming.cql.semanticanalyzer.parser.CQLParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectItemContext;

/**
 * select表达式语法结构遍历
 * 
 */
public class SelectExpressionVisitor extends AbsCQLParserBaseVisitor<SelectExpressionContext>
{
    private SelectExpressionContext context = null;
    
    /**
     * <默认构造函数>
     */
    public SelectExpressionVisitor()
    {
        context = new SelectExpressionContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected SelectExpressionContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SelectExpressionContext visitExpression(@NotNull CQLParser.ExpressionContext ctx)
    {
        ExpressionVisitor visitor = new ExpressionVisitor();
        context.setExpression(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SelectExpressionContext visitSelectAlias(@NotNull CQLParser.SelectAliasContext ctx)
    {
        SelectAliasVisitor visitor = new SelectAliasVisitor();
        context.setAlias(visitor.visit(ctx));
        return context;
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SelectExpressionContext visitStreamAllColumns(@NotNull CQLParser.StreamAllColumnsContext ctx)
    {
        StreamAllColumnsVisitor visitor = new StreamAllColumnsVisitor();
        context.setAllColumns(visitor.visit(ctx));
        return context;
    }
    
}
