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
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.Task;

/**
 * select表达式别名解析内容
 * 
 */
public class SelectAliasContext extends ParseContext
{
    private List<String> alias;
    
    /**
     * <默认构造函数>
     */
    public SelectAliasContext()
    {
        alias = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        if (alias.size() == 1)
        {
            return "AS " + alias.get(0);
        }
        
        return "AS (" + Joiner.on(", ").join(alias) + ")";
    }
    
    public List<String> getAlias()
    {
        return alias;
    }
    
    public void setAlias(List<String> alias)
    {
        this.alias = alias;
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
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        
    }
}
