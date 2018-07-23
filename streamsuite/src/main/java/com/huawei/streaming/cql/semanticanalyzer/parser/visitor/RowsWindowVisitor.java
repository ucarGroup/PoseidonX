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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.RowsWindowContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.WindowProperty;

/**
 * rows window 语法结构遍历
 * 
 */
public class RowsWindowVisitor extends AbsCQLParserBaseVisitor<RowsWindowContext>
{
    private RowsWindowContext context = null;
    
    /**
     * <默认构造函数>
     */
    public RowsWindowVisitor()
    {
        context = new RowsWindowContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected RowsWindowContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RowsWindowContext visitConstIntegerValue(@NotNull CQLParser.ConstIntegerValueContext ctx)
    {
        context.setRows(ctx.getText());
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RowsWindowContext visitWindowProperties(@NotNull CQLParser.WindowPropertiesContext ctx)
    {
        context.setWindowProperty(WindowProperty.valueOf(ctx.getText().toUpperCase(Locale.US)));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RowsWindowContext visitWindowDeterminer(@NotNull CQLParser.WindowDeterminerContext ctx)
    {
        WindowDeterminerVisitor visitor = new WindowDeterminerVisitor();
        context.setDeterminer(visitor.visit(ctx));
        return context;
    }
}
