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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.RangeBoundContext;

/**
 * range bound 语法遍历
 * 
 */
public class RangeBoundVisitor extends AbsCQLParserBaseVisitor<RangeBoundContext>
{
    private RangeBoundContext context = null;
    
    /**
     * <默认构造函数>
     */
    public RangeBoundVisitor()
    {
        context = new RangeBoundContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected RangeBoundContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RangeBoundContext visitRangeUnBound(@NotNull CQLParser.RangeUnBoundContext ctx)
    {
        context.setUnbounded(true);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RangeBoundContext visitRangeDay(@NotNull CQLParser.RangeDayContext ctx)
    {
        RangeDayVisitor visitor = new RangeDayVisitor();
        context.setDay(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RangeBoundContext visitRangeHour(@NotNull CQLParser.RangeHourContext ctx)
    {
        RangeHourVisitor visitor = new RangeHourVisitor();
        context.setHour(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RangeBoundContext visitRangeMinutes(@NotNull CQLParser.RangeMinutesContext ctx)
    {
        RangeMinutesVisitor visitor = new RangeMinutesVisitor();
        context.setMinutes(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RangeBoundContext visitRangeSeconds(@NotNull CQLParser.RangeSecondsContext ctx)
    {
        RangeSecondsVisitor visitor = new RangeSecondsVisitor();
        context.setSeconds(visitor.visit(ctx));
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RangeBoundContext visitRangeMilliSeconds(@NotNull CQLParser.RangeMilliSecondsContext ctx)
    {
        RangeMilliSecondsVisitor visitor = new RangeMilliSecondsVisitor();
        context.setMilliseconds(visitor.visit(ctx));
        return context;
    }
    
}
