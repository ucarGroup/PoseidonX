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
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.Task;

/**
 * 窗口限定字符描述内容
 * 
 */
public class WindowDeterminerContext extends ParseContext
{
    private PartitionbyDeterminerContext partitionByExpression;
    
    private SortbyDeterminerContext sortbyDeterminer;
    
    private TriggerbyDeterminerContext triggerbyExpression;
    
    private boolean isExcludeNow = false;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (partitionByExpression != null)
        {
            sb.append(partitionByExpression.toString());
        }
        if (sortbyDeterminer != null)
        {
            sb.append(sortbyDeterminer.toString());
        }
        if (triggerbyExpression != null)
        {
            sb.append(triggerbyExpression.toString());
        }
        if (isExcludeNow)
        {
            sb.append(" EXCLUDE NOW");
        }
        return sb.toString();
    }
    
    public PartitionbyDeterminerContext getPartitionByExpression()
    {
        return partitionByExpression;
    }
    
    public void setPartitionByExpression(PartitionbyDeterminerContext partitionByExpression)
    {
        this.partitionByExpression = partitionByExpression;
    }
    
    public SortbyDeterminerContext getSortbyDeterminer()
    {
        return sortbyDeterminer;
    }
    
    public void setSortbyDeterminer(SortbyDeterminerContext sortbyDeterminer)
    {
        this.sortbyDeterminer = sortbyDeterminer;
    }
    
    public TriggerbyDeterminerContext getTriggerbyExpression()
    {
        return triggerbyExpression;
    }
    
    public void setTriggerbyExpression(TriggerbyDeterminerContext triggerbyExpression)
    {
        this.triggerbyExpression = triggerbyExpression;
    }
    
    public boolean isExcludeNow()
    {
        return isExcludeNow;
    }
    
    public void setExcludeNow(boolean isexcludeNow)
    {
        this.isExcludeNow = isexcludeNow;
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
        walkExpression(walker, partitionByExpression);
        walkExpression(walker, sortbyDeterminer);
        walkExpression(walker, triggerbyExpression);
    }
    
}
