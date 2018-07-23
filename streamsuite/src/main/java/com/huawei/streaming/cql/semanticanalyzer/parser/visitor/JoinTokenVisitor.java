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

import com.huawei.streaming.api.opereators.JoinType;
import com.huawei.streaming.cql.semanticanalyzer.parser.CQLParser;

/**
 * join类型语法遍历
 * 
 */
public class JoinTokenVisitor extends AbsCQLParserBaseVisitor<JoinType>
{
    private JoinType context = null;
    
    /**
     * <默认构造函数>
     */
    public JoinTokenVisitor()
    {
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected JoinType defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public JoinType visitInnerJoin(@NotNull CQLParser.InnerJoinContext ctx)
    {
        context = JoinType.INNER_JOIN;
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public JoinType visitLeftJoin(@NotNull CQLParser.LeftJoinContext ctx)
    {
        context = JoinType.LEFT_OUTER_JOIN;
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public JoinType visitRightJoin(@NotNull CQLParser.RightJoinContext ctx)
    {
        context = JoinType.RIGHT_OUTER_JOIN;
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public JoinType visitFullJoin(@NotNull CQLParser.FullJoinContext ctx)
    {
        context = JoinType.FULL_OUTER_JOIN;
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public JoinType visitCrossJoin(@NotNull CQLParser.CrossJoinContext ctx)
    {
        context = JoinType.CROSS_JOIN;
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public JoinType visitNaturalJoin(@NotNull CQLParser.NaturalJoinContext ctx)
    {
        context = JoinType.NATURAL_JOIN;
        return context;
    }
    
}
