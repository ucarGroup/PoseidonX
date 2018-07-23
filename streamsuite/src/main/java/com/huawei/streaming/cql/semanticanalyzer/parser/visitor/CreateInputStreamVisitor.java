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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.CreateInputStatementContext;

/**
 * create input stream 语法遍历
 * 
 */
public class CreateInputStreamVisitor extends AbsCQLParserBaseVisitor<CreateInputStatementContext>
{
    private CreateInputStatementContext context = null;
    
    /**
     * <默认构造函数>
     */
    public CreateInputStreamVisitor()
    {
        context = new CreateInputStatementContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected CreateInputStatementContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CreateInputStatementContext visitStreamName(@NotNull CQLParser.StreamNameContext ctx)
    {
        context.setStreamName(ctx.getText().toLowerCase(Locale.US));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CreateInputStatementContext visitColumnNameTypeList(@NotNull CQLParser.ColumnNameTypeListContext ctx)
    {
        ColumnNameTypeListVisitor visitor = new ColumnNameTypeListVisitor();
        context.setColumns(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CreateInputStatementContext visitComment(@NotNull CQLParser.CommentContext ctx)
    {
        //忽略注释
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CreateInputStatementContext visitSerdeClass(@NotNull CQLParser.SerdeClassContext ctx)
    {
        ClassNameVisitor visitor = new ClassNameVisitor();
        context.setDeserClassName(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CreateInputStatementContext visitSerdeProperties(@NotNull CQLParser.SerdePropertiesContext ctx)
    {
        StreamPropertiesVisitor visitor = new StreamPropertiesVisitor();
        context.setDeserProperties(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CreateInputStatementContext visitSourceClause(@NotNull CQLParser.SourceClauseContext ctx)
    {
        ClassNameVisitor visitor = new ClassNameVisitor();
        context.setSourceClassName(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CreateInputStatementContext visitSourceProperties(@NotNull CQLParser.SourcePropertiesContext ctx)
    {
        StreamPropertiesVisitor visitor = new StreamPropertiesVisitor();
        context.setSourceProperties(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CreateInputStatementContext visitParallelClause(@NotNull CQLParser.ParallelClauseContext ctx)
    {
        ParallelClauseVisitor visitor = new ParallelClauseVisitor();
        context.setParallelNumber(visitor.visit(ctx));
        return context;
    }
}
