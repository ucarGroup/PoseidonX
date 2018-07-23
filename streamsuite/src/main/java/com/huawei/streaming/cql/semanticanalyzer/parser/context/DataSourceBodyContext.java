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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.DataSourceQueryArgumentsAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.Task;

/**
 * 创建数据源解析内容
 * 
 */
public class DataSourceBodyContext extends ParseContext
{
    private String dataSourceName;
    
    private String alia;

    private List<ExpressionContext> queryarguments;
    
    private ColumnNameTypeListContext schemaColumns;
    
    /**
     * <默认构造函数>
     */
    public DataSourceBodyContext()
    {
        queryarguments = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("DATASOURCE " + dataSourceName);
        sb.append("[");
        sb.append(" SCHEMA (" + schemaColumns.toString() + ")");
        sb.append(" QUERY(" + Joiner.on(", ").join(queryarguments) + ")");
        sb.append("]");
        if (alia != null)
        {
            sb.append(" " + alia);
        }
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException
    {
        return new DataSourceQueryArgumentsAnalyzer(this);
    }
    
    public String getDataSourceName()
    {
        return dataSourceName;
    }
    
    public void setDataSourceName(String dataSourceName)
    {
        this.dataSourceName = dataSourceName;
    }
    
    public List<ExpressionContext> getQueryarguments()
    {
        return queryarguments;
    }
    
    public void setQueryarguments(List<ExpressionContext> queryarguments)
    {
        this.queryarguments = queryarguments;
    }
    
    public ColumnNameTypeListContext getSchemaColumns()
    {
        return schemaColumns;
    }
    
    public void setSchemaColumns(ColumnNameTypeListContext schemaColumns)
    {
        this.schemaColumns = schemaColumns;
    }
    
    public String getAlia()
    {
        return alia;
    }
    
    public void setAlia(String alia)
    {
        this.alia = alia;
    }
    
}
