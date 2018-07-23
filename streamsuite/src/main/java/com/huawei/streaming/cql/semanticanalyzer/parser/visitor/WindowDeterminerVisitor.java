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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.WindowDeterminerContext;

/**
 * window限定词遍历
 * 
 */
public class WindowDeterminerVisitor extends AbsCQLParserBaseVisitor<WindowDeterminerContext>
{
    private WindowDeterminerContext context = null;
    
    /**
     * <默认构造函数>
     */
    public WindowDeterminerVisitor()
    {
        context = new WindowDeterminerContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected WindowDeterminerContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public WindowDeterminerContext visitPartitionbyDeterminer(@NotNull CQLParser.PartitionbyDeterminerContext ctx)
    {
        PartitionByDeterminerVisitor visitor = new PartitionByDeterminerVisitor();
        context.setPartitionByExpression(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public WindowDeterminerContext visitSortbyDeterminer(@NotNull CQLParser.SortbyDeterminerContext ctx)
    {
        SortbyDeterminerVisitor visitor = new SortbyDeterminerVisitor();
        context.setSortbyDeterminer(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public WindowDeterminerContext visitTriggerbyDeterminer(@NotNull CQLParser.TriggerbyDeterminerContext ctx)
    {
        TriggerbyDeterminerVisitor visitor = new TriggerbyDeterminerVisitor();
        context.setTriggerbyExpression(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public WindowDeterminerContext visitExcludeNowDeterminer(@NotNull CQLParser.ExcludeNowDeterminerContext ctx)
    {
        context.setExcludeNow(true);
        return context;
    }
    
}
