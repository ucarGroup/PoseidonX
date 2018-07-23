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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext;

import java.util.List;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.MultiSelectContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * select语句的语义分析内容
 *
 */
public class SelectWithOutFromAnalyzeContext extends AnalyzeContext
{
    private SelectClauseAnalyzeContext selectClauseContext;
    
    private FilterClauseAnalzyeContext whereClauseContext;
    
    private SelectClauseAnalyzeContext groupbyClauseContext;

    private OrderByClauseAnalyzeContext orderbyClauseContext;
    
    private FilterClauseAnalzyeContext havingClauseContext;
    
    private LimitClauseAnalzyeContext limitClauseContext;
    
    private MultiSelectContext context;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParseContext(ParseContext parseContext)
    {
        context = (MultiSelectContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<Schema> getCreatedSchemas()
    {
        List<Schema> schemas = Lists.newArrayList();
        schemas.addAll(selectClauseContext.getCreatedSchemas());
        return schemas;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return context.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void validateParseContext()
        throws SemanticAnalyzerException
    {
        // TODO Auto-generated method stub
        
    }
    
    public FilterClauseAnalzyeContext getWhereClauseContext()
    {
        return whereClauseContext;
    }
    
    public void setWhereClauseContext(FilterClauseAnalzyeContext whereClauseContext)
    {
        this.whereClauseContext = whereClauseContext;
    }
    
    public SelectClauseAnalyzeContext getGroupbyClauseContext()
    {
        return groupbyClauseContext;
    }
    
    public void setGroupbyClauseContext(SelectClauseAnalyzeContext groupbyClauseContext)
    {
        this.groupbyClauseContext = groupbyClauseContext;
    }
    
    public OrderByClauseAnalyzeContext getOrderbyClauseContext()
    {
        return orderbyClauseContext;
    }
    
    public void setOrderbyClauseContext(OrderByClauseAnalyzeContext orderbyClauseContext)
    {
        this.orderbyClauseContext = orderbyClauseContext;
    }
    
    public FilterClauseAnalzyeContext getHavingClauseContext()
    {
        return havingClauseContext;
    }
    
    public void setHavingClauseContext(FilterClauseAnalzyeContext havingClauseContext)
    {
        this.havingClauseContext = havingClauseContext;
    }
    
    public LimitClauseAnalzyeContext getLimitClauseContext()
    {
        return limitClauseContext;
    }
    
    public void setLimitClauseContext(LimitClauseAnalzyeContext limitClauseContext)
    {
        this.limitClauseContext = limitClauseContext;
    }
    
    public void setSelectClauseContext(SelectClauseAnalyzeContext selectClauseContext)
    {
        this.selectClauseContext = selectClauseContext;
    }
    
    public SelectClauseAnalyzeContext getSelectClauseContext()
    {
        return selectClauseContext;
    }
}
