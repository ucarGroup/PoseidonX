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

package com.huawei.streaming.cql.semanticanalyzer.parser.context;

import java.util.List;

import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.SelectStatementAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.LazyTask;
import com.huawei.streaming.cql.tasks.Task;

/**
 * select语句解析内容
 * 
 */
public class SelectStatementContext extends ParseContext
{
    private SelectClauseContext select;
    
    private FromClauseContext from;
    
    private WhereClauseContext where;
    
    private GroupbyClauseContext groupby;
    
    private HavingClauseContext having;

    private OrderbyClauseContext orderby;
    
    private LimitClauseContext limit;
    
    private ParallelClauseContext parallel;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(select.toString());
        apendFromClause(sb);
        apendWhereClause(sb);
        apendGroupbyClause(sb);
        apendHavingClause(sb);
        apendOrderbyClause(sb);
        apendLimitClause(sb);
        apendParallelClause(sb);
        return sb.toString();
    }
    
    private void apendParallelClause(StringBuilder sb)
    {
        if (parallel != null)
        {
            sb.append(" " + parallel.toString());
        }
    }
    
    private void apendLimitClause(StringBuilder sb)
    {
        if (limit != null)
        {
            sb.append(" " + limit.toString());
        }
    }
    
    private void apendOrderbyClause(StringBuilder sb)
    {
        if (orderby != null)
        {
            sb.append(" " + orderby.toString());
        }
    }

    private void apendHavingClause(StringBuilder sb)
    {
        if (having != null)
        {
            sb.append(" " + having.toString());
        }
    }
    
    private void apendGroupbyClause(StringBuilder sb)
    {
        if (groupby != null)
        {
            sb.append(" " + groupby.toString());
        }
    }
    
    private void apendWhereClause(StringBuilder sb)
    {
        if (where != null)
        {
            sb.append(" " + where.toString());
        }
    }
    
    private void apendFromClause(StringBuilder sb)
    {
        if (from != null)
        {
            sb.append(" " + from.toString());
        }
    }
    
    public SelectClauseContext getSelect()
    {
        return select;
    }
    
    public void setSelect(SelectClauseContext select)
    {
        this.select = select;
    }
    
    public FromClauseContext getFrom()
    {
        return from;
    }
    
    public void setFrom(FromClauseContext from)
    {
        this.from = from;
    }
    
    public WhereClauseContext getWhere()
    {
        return where;
    }
    
    public void setWhere(WhereClauseContext where)
    {
        this.where = where;
    }
    
    public GroupbyClauseContext getGroupby()
    {
        return groupby;
    }
    
    public void setGroupby(GroupbyClauseContext groupby)
    {
        this.groupby = groupby;
    }
    
    public HavingClauseContext getHaving()
    {
        return having;
    }
    
    public void setHaving(HavingClauseContext having)
    {
        this.having = having;
    }

    public OrderbyClauseContext getOrderby()
    {
        return orderby;
    }
    
    public void setOrderby(OrderbyClauseContext orderby)
    {
        this.orderby = orderby;
    }
    
    public LimitClauseContext getLimit()
    {
        return limit;
    }
    
    public void setLimit(LimitClauseContext limit)
    {
        this.limit = limit;
    }
    
    public ParallelClauseContext getParallel()
    {
        return parallel;
    }
    
    public void setParallel(ParallelClauseContext parallel)
    {
        this.parallel = parallel;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        return new LazyTask();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException
    {
        return new SelectStatementAnalyzer(this);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, select);
        walkExpression(walker, from);
        walkExpression(walker, where);
        walkExpression(walker, orderby);
        walkExpression(walker, groupby);
        walkExpression(walker, having);
        walkExpression(walker, limit);
        walkExpression(walker, parallel);
    }
    
}
