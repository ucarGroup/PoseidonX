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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc;

import java.util.List;

import com.huawei.streaming.api.streams.Schema;

/**
 * 查询时使用到的数据源描述信息
 * 
 */
public class DatasourceBodyDesc implements ExpressionDescribe
{
    private List<String> queryArguments;
    
    private String datasourceName;
    
    private Schema schema;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return datasourceName;
    }
    
    public String getDatasourceName()
    {
        return datasourceName;
    }
    
    public void setDatasourceName(String datasourceName)
    {
        this.datasourceName = datasourceName;
    }
    
    public Schema getSchema()
    {
        return schema;
    }
    
    public void setSchema(Schema schema)
    {
        this.schema = schema;
    }
    
    public List<String> getQueryArguments()
    {
        return queryArguments;
    }
    
    public void setQueryArguments(List<String> queryArguments)
    {
        this.queryArguments = queryArguments;
    }
    
}
