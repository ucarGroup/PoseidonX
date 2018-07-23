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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FunctionContext;

/**
 * function语法遍历
 * 
 */
public class FunctionVisitor extends AbsCQLParserBaseVisitor<FunctionContext>
{
    private FunctionContext context = null;
    
    /**
     * <默认构造函数>
     */
    public FunctionVisitor()
    {
        context = new FunctionContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected FunctionContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public FunctionContext visitFunctionName(@NotNull CQLParser.FunctionNameContext ctx)
    {
        context.setName(ctx.getText().toLowerCase(Locale.US));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public FunctionContext visitDistinct(@NotNull CQLParser.DistinctContext ctx)
    {
        context.setDistinct(true);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public FunctionContext visitExpression(@NotNull CQLParser.ExpressionContext ctx)
    {
        ExpressionVisitor visitor = new ExpressionVisitor();
        context.getArguments().add(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public FunctionContext visitStreamAllColumns(@NotNull CQLParser.StreamAllColumnsContext ctx)
    {
        StreamAllColumnsVisitor visitor = new StreamAllColumnsVisitor();
        context.setAllColumns(visitor.visit(ctx));
        return context;
    }
}
