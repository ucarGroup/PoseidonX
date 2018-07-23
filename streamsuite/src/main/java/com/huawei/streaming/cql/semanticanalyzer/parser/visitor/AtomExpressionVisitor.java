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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.AtomExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ConstantContext;

/**
 * 原子表达式解析结果
 * 
 * 对一些原子的，不可分割的表达式的解析
 * 包含Null、常量、函数、Cast表达式，Case When表达式，流名称或者别名的解析
 * 以及带括号的表达式的解析
 * 表达式优先级规定，括号可以改变表达式的优先级，所以带括号的表达式优先解析
 * 
 * 语法如下：
 * atomExpression
 *   :   constNull
 *   |   constant
 *   |   function
 *   |   castExpression
 *   |   caseExpression *   |   whenExpression
 *   |   streamName
 *   |   streamNameOrAlias
 *   |   expressionWithLaparen
 *   ;
 * 
 */
public class AtomExpressionVisitor extends AbsCQLParserBaseVisitor<AtomExpressionContext>
{
    /**
     * 原子类型表达式解析结果
     */
    private AtomExpressionContext context = null;
    
    /**
     * <默认构造函数>
     */
    public AtomExpressionVisitor()
    {
        super();
        context = new AtomExpressionContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AtomExpressionContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AtomExpressionContext visitConstNull(@NotNull CQLParser.ConstNullContext ctx)
    {
        ConstantContext contant = new ConstantContext(null, null);
        context.setConstant(contant);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AtomExpressionContext visitConstant(@NotNull CQLParser.ConstantContext ctx)
    {
        ConstantVisitor visitor = new ConstantVisitor();
        context.setConstant(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AtomExpressionContext visitFunction(@NotNull CQLParser.FunctionContext ctx)
    {
        FunctionVisitor visitor = new FunctionVisitor();
        context.setFunction(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AtomExpressionContext visitCastExpression(@NotNull CQLParser.CastExpressionContext ctx)
    {
        CastExpressionVisitor visitor = new CastExpressionVisitor();
        context.setCastExpression(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AtomExpressionContext visitCaseExpression(@NotNull CQLParser.CaseExpressionContext ctx)
    {
        CaseExpressionVisitor visitor = new CaseExpressionVisitor();
        context.setCaseExpression(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AtomExpressionContext visitWhenExpression(@NotNull CQLParser.WhenExpressionContext ctx)
    {
        WhenExpressionVisitor visitor = new WhenExpressionVisitor();
        context.setWhenExpression(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AtomExpressionContext visitColumnName(@NotNull CQLParser.ColumnNameContext ctx)
    {
        context.setColumnName(ctx.getText().toLowerCase(Locale.US));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AtomExpressionContext visitExpressionWithLaparen(@NotNull CQLParser.ExpressionWithLaparenContext ctx)
    {
        ExpressionWithLaparenVisitor visitor = new ExpressionWithLaparenVisitor();
        context.setExpressionWithLaparen(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AtomExpressionContext visitExpressionPrevious(@NotNull CQLParser.ExpressionPreviousContext ctx)
    {
        ExpressionPreviousVisitor visitor = new ExpressionPreviousVisitor();
        context.setPrevious(visitor.visit(ctx));
        return context;
    }
    
}
