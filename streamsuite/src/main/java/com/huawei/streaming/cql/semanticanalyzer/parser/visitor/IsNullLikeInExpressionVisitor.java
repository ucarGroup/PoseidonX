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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.IsNullLikeBetweenInExpressionContext;

/**
 * is null,like,between ,in 表达式遍历
 * 
 */
public class IsNullLikeInExpressionVisitor extends AbsCQLParserBaseVisitor<IsNullLikeBetweenInExpressionContext>
{
    private IsNullLikeBetweenInExpressionContext context = null;
    
    /**
     * <默认构造函数>
     */
    public IsNullLikeInExpressionVisitor()
    {
        context = new IsNullLikeBetweenInExpressionContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected IsNullLikeBetweenInExpressionContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IsNullLikeBetweenInExpressionContext visitBinaryExpression(@NotNull CQLParser.BinaryExpressionContext ctx)
    {
        BinaryExpressionVisitor visitor = new BinaryExpressionVisitor();
        context.setLeft(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IsNullLikeBetweenInExpressionContext visitNullCondition(@NotNull CQLParser.NullConditionContext ctx)
    {
        NullConditionVisitor visitor = new NullConditionVisitor();
        context.setIsNullExpression(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IsNullLikeBetweenInExpressionContext visitExpressionLike(@NotNull CQLParser.ExpressionLikeContext ctx)
    {
        ExpressionLikeVisitor visitor = new ExpressionLikeVisitor();
        context.setLike(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IsNullLikeBetweenInExpressionContext visitExpressionBetween(@NotNull CQLParser.ExpressionBetweenContext ctx)
    {
        ExpressionBetweenVisitor visitor = new ExpressionBetweenVisitor();
        context.setBetween(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IsNullLikeBetweenInExpressionContext visitExpressionIn(@NotNull CQLParser.ExpressionInContext ctx)
    {
        ExpressionInVisitor visitor = new ExpressionInVisitor();
        context.setIn(visitor.visit(ctx));
        return context;
    }
}
