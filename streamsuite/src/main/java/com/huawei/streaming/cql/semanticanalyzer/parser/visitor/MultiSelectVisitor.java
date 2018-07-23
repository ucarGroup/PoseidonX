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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.MultiSelectContext;

/**
 * 多级insert select 主体语句语法遍历
 *
 */
public class MultiSelectVisitor extends AbsCQLParserBaseVisitor<MultiSelectContext>
{
    private MultiSelectContext context = null;
    
    /**
     * <默认构造函数>
     */
    public MultiSelectVisitor()
    {
        context = new MultiSelectContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected MultiSelectContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public MultiSelectContext visitSelectClause(@NotNull CQLParser.SelectClauseContext ctx)
    {
        SelectClauseVisitor visitor = new SelectClauseVisitor();
        context.setSelect(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public MultiSelectContext visitWhereClause(@NotNull CQLParser.WhereClauseContext ctx)
    {
        WhereClauseVisitor visitor = new WhereClauseVisitor();
        context.setWhere(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public MultiSelectContext visitGroupByClause(@NotNull CQLParser.GroupByClauseContext ctx)
    {
        GroupbyClauseVisitor visitor = new GroupbyClauseVisitor();
        context.setGroupby(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public MultiSelectContext visitHavingClause(@NotNull CQLParser.HavingClauseContext ctx)
    {
        HavingClauseVisitor visitor = new HavingClauseVisitor();
        context.setHaving(visitor.visit(ctx));
        return context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MultiSelectContext visitOrderByClause(@NotNull CQLParser.OrderByClauseContext ctx)
    {
        OrderbyClauseVisitor visitor = new OrderbyClauseVisitor();
        context.setOrderby(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public MultiSelectContext visitLimitClause(@NotNull CQLParser.LimitClauseContext ctx)
    {
        LimitClauseVisitor visitor = new LimitClauseVisitor();
        context.setLimit(visitor.visit(ctx));
        return context;
    }
    
}
