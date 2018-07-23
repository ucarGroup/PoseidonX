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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionBetweenContext;

/**
 * between表达式遍历
 * 
 */
public class ExpressionBetweenVisitor extends AbsCQLParserBaseVisitor<ExpressionBetweenContext>
{
    private ExpressionBetweenContext context = null;
    
    /**
     * <默认构造函数>
     */
    public ExpressionBetweenVisitor()
    {
        context = new ExpressionBetweenContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ExpressionBetweenContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ExpressionBetweenContext visitIdentifierNot(@NotNull CQLParser.IdentifierNotContext ctx)
    {
        context.setNot(true);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ExpressionBetweenContext visitExpressionBetweenMinValue(
        @NotNull CQLParser.ExpressionBetweenMinValueContext ctx)
    {
        ExpressionBetweenMinValueVisitor visitor = new ExpressionBetweenMinValueVisitor();
        context.setMinValue(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ExpressionBetweenContext visitExpressionBetweenMaxValue(
        @NotNull CQLParser.ExpressionBetweenMaxValueContext ctx)
    {
        ExpressionBetweenMaxValueVisitor visitor = new ExpressionBetweenMaxValueVisitor();
        context.setMaxValue(visitor.visit(ctx));
        return context;
    }
    
}
