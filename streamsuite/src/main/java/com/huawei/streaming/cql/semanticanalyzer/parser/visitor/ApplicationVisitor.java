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
import org.antlr.v4.runtime.tree.TerminalNode;

import com.huawei.streaming.cql.semanticanalyzer.parser.CQLParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * 应用程序语法遍历
 * 
 */
public class ApplicationVisitor extends AbsCQLParserBaseVisitor<ParseContext>
{
    private ParseContext context = null;
    
    /**
     * <默认构造函数>
     */
    public ApplicationVisitor()
    {
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ParseContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitDdlStatement(@NotNull CQLParser.DdlStatementContext ctx)
    {
        DDLStatementVisitor visitor = new DDLStatementVisitor();
        context = visitor.visit(ctx);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitExecStatement(@NotNull CQLParser.ExecStatementContext ctx)
    {
        ExecStatementVisitor visitor = new ExecStatementVisitor();
        context = visitor.visit(ctx);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitExpression(@NotNull CQLParser.ExpressionContext ctx)
    {
        ExpressionVisitor visitor = new ExpressionVisitor();
        context = visitor.visit(ctx);
        return context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitTerminal(@NotNull TerminalNode node) {
        return context;
    }
    
}
