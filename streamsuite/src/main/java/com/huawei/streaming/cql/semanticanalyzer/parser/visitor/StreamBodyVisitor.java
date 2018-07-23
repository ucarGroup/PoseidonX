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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.StreamBodyContext;

/**
 * from 子句中的流相关语法内容
 * 
 */
public class StreamBodyVisitor extends AbsCQLParserBaseVisitor<StreamBodyContext>
{
    private StreamBodyContext context = null;
    
    /**
     * <默认构造函数>
     */
    public StreamBodyVisitor()
    {
        context = new StreamBodyContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected StreamBodyContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public StreamBodyContext visitStreamSource(@NotNull CQLParser.StreamSourceContext ctx)
    {
        StreamSourceVisitor visitor = new StreamSourceVisitor();
        context.setStreamSource(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public StreamBodyContext visitFilterBeforeWindow(@NotNull CQLParser.FilterBeforeWindowContext ctx)
    {
        FilterBeforeWindowVisitor visitor = new FilterBeforeWindowVisitor();
        context.setFilterBeforeWindow(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public StreamBodyContext visitWindowSource(@NotNull CQLParser.WindowSourceContext ctx)
    {
        WindowSourceVisitor visitor = new WindowSourceVisitor();
        context.setWindow(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public StreamBodyContext visitSourceAlias(@NotNull CQLParser.SourceAliasContext ctx)
    {
        SourceAliasVisitor visitor = new SourceAliasVisitor();
        context.setAlias(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public StreamBodyContext visitUnidirection(@NotNull CQLParser.UnidirectionContext ctx)
    {
        context.setUnidirection(true);
        return context;
    }
    
}
