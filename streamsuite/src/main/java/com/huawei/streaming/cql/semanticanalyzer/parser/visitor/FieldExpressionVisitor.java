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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FieldExpressionContext;

/**
 * 
 * 字段表达式遍历
 * 解析如S1.id这样的语法
 * S1既可以是流名称，也可以是流的别名
 *  
 *  语法如下：
 *  fieldExpression
 *   :    atomExpression (DOT columnName)*
 *   ;
 * 
 */
public class FieldExpressionVisitor extends AbsCQLParserBaseVisitor<FieldExpressionContext>
{
    /**
     * 字段表达式解析结果
     */
    private FieldExpressionContext context = null;
    
    /**
     * <默认构造函数>
     */
    public FieldExpressionVisitor()
    {
        super();
        context = new FieldExpressionContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected FieldExpressionContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public FieldExpressionContext visitAtomExpression(@NotNull CQLParser.AtomExpressionContext ctx)
    {
        AtomExpressionVisitor visitor = new AtomExpressionVisitor();
        context.setAtomExpression(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public FieldExpressionContext visitStreamNameOrAlias(@NotNull CQLParser.StreamNameOrAliasContext ctx)
    {
        context.setStreamNameOrAlias(ctx.getText().toLowerCase(Locale.US));
        return context;
    }
    
}
