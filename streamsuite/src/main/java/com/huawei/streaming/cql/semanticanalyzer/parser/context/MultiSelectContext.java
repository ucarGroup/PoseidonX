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
import com.huawei.streaming.cql.semanticanalyzer.SelectWithOutFromAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.LazyTask;
import com.huawei.streaming.cql.tasks.Task;

/**
 * 多级Insert 语句语法解析内容
 *
 */
public class MultiSelectContext extends ParseContext
{
    
    private SelectClauseContext select;
    
    private WhereClauseContext where;
    
    private GroupbyClauseContext groupby;
    
    private HavingClauseContext having;

    private OrderbyClauseContext orderby;
    
    private LimitClauseContext limit;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(select.toString());
        appendWhereClause(sb);
        appendGroupbyClause(sb);
        appendHavingClause(sb);
        appendOrderbyClause(sb);
        appendLimitClause(sb);
        return sb.toString();
    }
    
    public SelectClauseContext getSelect()
    {
        return select;
    }
    
    public void setSelect(SelectClauseContext select)
    {
        this.select = select;
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
        return new SelectWithOutFromAnalyzer(this);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, select);
        walkExpression(walker, where);
        walkExpression(walker, orderby);
        walkExpression(walker, groupby);
        walkExpression(walker, having);
        walkExpression(walker, limit);
    }
    
    private void appendLimitClause(StringBuilder sb)
    {
        if (limit != null)
        {
            sb.append(" " + limit.toString());
        }
    }
    
    private void appendOrderbyClause(StringBuilder sb)
    {
        if (orderby != null)
        {
            sb.append(" " + orderby.toString());
        }
    }
    
    private void appendHavingClause(StringBuilder sb)
    {
        if (having != null)
        {
            sb.append(" " + having.toString());
        }
    }
    
    private void appendGroupbyClause(StringBuilder sb)
    {
        if (groupby != null)
        {
            sb.append(" " + groupby.toString());
        }
    }
    
    private void appendWhereClause(StringBuilder sb)
    {
        if (where != null)
        {
            sb.append(" " + where.toString());
        }
    }
    
}
