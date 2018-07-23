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
import com.huawei.streaming.cql.semanticanalyzer.SelectClauseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.Task;

/**
 * select子句解析内容
 * 
 */
public class SelectClauseContext extends ParseContext
{
    
    private boolean isDistinct = false;
    
    private List<SelectItemContext> selectItems;
    
    /**
     * <默认构造函数>
     */
    public SelectClauseContext()
    {
        selectItems = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(isDistinct ? "DISTINCT " : "");
        sb.append(Joiner.on(", ").join(selectItems));
        return sb.toString();
    }
    
    public boolean isDistinct()
    {
        return isDistinct;
    }
    
    public void setDistinct(boolean isdistinct)
    {
        this.isDistinct = isdistinct;
    }
    
    public List<SelectItemContext> getSelectItems()
    {
        return selectItems;
    }
    
    public void setSelectItems(List<SelectItemContext> selectItems)
    {
        this.selectItems = selectItems;
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
        return new SelectClauseAnalyzer(this);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        for (SelectItemContext item : selectItems)
        {
            walkExpression(walker, item);
        }
    }
}
