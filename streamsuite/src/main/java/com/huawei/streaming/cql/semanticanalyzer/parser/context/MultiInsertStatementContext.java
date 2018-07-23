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
import com.huawei.streaming.cql.semanticanalyzer.MultiInsertStatementAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.LazyTask;
import com.huawei.streaming.cql.tasks.Task;

/**
 * 多级Insert 语句语法解析内容
 *
 */
public class MultiInsertStatementContext extends ParseContext
{
    private FromClauseContext from;
    
    private List<MultiInsertContext> inserts;
    
    private ParallelClauseContext parallel;
    
    /**
     * <默认构造函数>
     */
    public MultiInsertStatementContext()
    {
        inserts = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        if (parallel != null)
        {
            return from.toString() + " " + Joiner.on(" ").join(inserts) + " " + parallel.toString();
        }
        else
        {
            return from.toString() + " " + Joiner.on(" ").join(inserts);
        }
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
        return new MultiInsertStatementAnalyzer(this);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, from);
        walkExpression(walker, parallel);
        for (MultiInsertContext select : inserts)
        {
            walkExpression(walker, select);
            
        }
    }
    
    public FromClauseContext getFrom()
    {
        return from;
    }
    
    public void setFrom(FromClauseContext from)
    {
        this.from = from;
    }
    
    public ParallelClauseContext getParallel()
    {
        return parallel;
    }
    
    public void setParallel(ParallelClauseContext parallel)
    {
        this.parallel = parallel;
    }
    
    public List<MultiInsertContext> getInserts()
    {
        return inserts;
    }
    
    public void setInserts(List<MultiInsertContext> inserts)
    {
        this.inserts = inserts;
    }
}
