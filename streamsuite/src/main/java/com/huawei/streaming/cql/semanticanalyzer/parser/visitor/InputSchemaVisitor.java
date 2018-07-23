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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.InputSchemaStatementContext;

/**
 * 自定义算子输入schema 语法遍历
 */
public class InputSchemaVisitor extends AbsCQLParserBaseVisitor<InputSchemaStatementContext>
{
    
    private InputSchemaStatementContext context;
    
    /**
     * 输入schema遍历
     */
    public InputSchemaVisitor()
    {
        context = new InputSchemaStatementContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected InputSchemaStatementContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public InputSchemaStatementContext visitColumnNameTypeList(@NotNull CQLParser.ColumnNameTypeListContext ctx)
    {
        ColumnNameTypeListVisitor visitor = new ColumnNameTypeListVisitor();
        context.setInputSchema(visitor.visit(ctx));
        return context;
    }
}
