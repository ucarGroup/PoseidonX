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

import java.util.Locale;

import org.antlr.v4.runtime.misc.NotNull;

import com.huawei.streaming.cql.semanticanalyzer.parser.CQLParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.UsingStatementContext;

/**
 * 使用自定义算子  using子句语法遍历
 */
public class UsingStatementVisitor extends AbsCQLParserBaseVisitor<UsingStatementContext>
{
    private UsingStatementContext context = null;
    
    /**
     * using 语句遍历
     */
    public UsingStatementVisitor()
    {
        context = new UsingStatementContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected UsingStatementContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public UsingStatementContext visitOperatorName(@NotNull CQLParser.OperatorNameContext ctx)
    {
        context.setOperatorName(ctx.getText().toLowerCase(Locale.US));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public UsingStatementContext visitStreamName(@NotNull CQLParser.StreamNameContext ctx)
    {
        context.setStreamName(ctx.getText().toLowerCase(Locale.US));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public UsingStatementContext visitDistributeClause(@NotNull CQLParser.DistributeClauseContext ctx)
    {
        DistributeClauseVisitor visitor = new DistributeClauseVisitor();
        context.setDistributeContext(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public UsingStatementContext visitParallelClause(@NotNull CQLParser.ParallelClauseContext ctx)
    {
        ParallelClauseVisitor visitor = new ParallelClauseVisitor();
        context.setParallelNumber(visitor.visit(ctx));
        return context;
    }
    
}
