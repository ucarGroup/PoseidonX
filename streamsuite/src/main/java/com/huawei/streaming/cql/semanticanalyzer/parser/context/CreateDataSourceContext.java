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
import com.huawei.streaming.cql.semanticanalyzer.CreateDatasourceAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.LazyTask;
import com.huawei.streaming.cql.tasks.Task;

/**
 * 创建数据源解析内容
 * 
 */
public class CreateDataSourceContext extends ParseContext
{
    private String dataSourceName;
    
    private ClassNameContext dataSourceClassName;
    
    private StreamPropertiesContext dataSourceProperties;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATASOURCE " + dataSourceName);
        sb.append(" SOURCE " + dataSourceClassName.toString() + " ");

        if(dataSourceProperties != null)
        {
            sb.append(dataSourceProperties.toString());
        }
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, dataSourceProperties);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        Task task = new LazyTask();
        return task;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException
    {
        return new CreateDatasourceAnalyzer(this);
    }
    
    public String getDataSourceName()
    {
        return dataSourceName;
    }
    
    public void setDataSourceName(String dataSourceName)
    {
        this.dataSourceName = dataSourceName;
    }
    
    public ClassNameContext getDataSourceClassName()
    {
        return dataSourceClassName;
    }
    
    public void setDataSourceClassName(ClassNameContext dataSourceClassName)
    {
        this.dataSourceClassName = dataSourceClassName;
    }
    
    public StreamPropertiesContext getDataSourceProperties()
    {
        return dataSourceProperties;
    }
    
    public void setDataSourceProperties(StreamPropertiesContext dataSourceProperties)
    {
        this.dataSourceProperties = dataSourceProperties;
    }
    
}
