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
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.CreateDataSourceContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * create datasource 语义分析结果内容
 *
 */
public class CreateDataSourceAnalyzeContext extends AnalyzeContext
{
    private CreateDataSourceContext context;

    private String datasourcClass;

    private TreeMap<String, String> datasourceConfigs = Maps.newTreeMap();
    
    public void setDatasourceConfigs(TreeMap<String, String> datasourceConfigs)
    {
        this.datasourceConfigs = datasourceConfigs;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParseContext(ParseContext parseContext)
    {
        context = (CreateDataSourceContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void validateParseContext()
        throws SemanticAnalyzerException
    {
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<Schema> getCreatedSchemas()
    {
        return Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return context.toString();
    }
    
    public String getDataSourceName()
    {
        return context.getDataSourceName();
    }

    public void setDataSourceClass(String clazz)
    {
        this.datasourcClass = clazz;
    }

    /**
     * 获取数据源的类
     *
     */
    public String getDataSourceClass()
    {
        return this.datasourcClass;
    }
    
    /**
     * 获取数据源配置属性
     *
     */
    public TreeMap<String, String> getDataSourceConfig()
    {
        return datasourceConfigs;
    }
}
