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
import com.huawei.streaming.cql.tasks.LazyTask;
import com.huawei.streaming.cql.tasks.Task;

/**
 * 使用自定义算子 using子句分析
 */
public class UsingStatementContext extends ParseContext
{
    
    private String operatorName;
    
    private String streamName;
    
    private DistributeClauseContext distributeContext;
    
    private ParallelClauseContext parallelNumber;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("USING OPERATOR ").append(operatorName);
        sb.append(" FROM ").append(streamName);
        
        if (distributeContext != null)
        {
            sb.append(" ").append(distributeContext.toString());
        }
        
        if (parallelNumber != null)
        {
            sb.append(" ").append(parallelNumber.toString());
        }
        
        return sb.toString();
    }
    
    public String getOperatorName()
    {
        return operatorName;
    }
    
    public void setOperatorName(String operatorName)
    {
        this.operatorName = operatorName;
    }
    
    public String getStreamName()
    {
        return streamName;
    }
    
    public void setStreamName(String streamName)
    {
        this.streamName = streamName;
    }
    
    public DistributeClauseContext getDistributeContext()
    {
        return distributeContext;
    }
    
    public void setDistributeContext(DistributeClauseContext distributeContext)
    {
        this.distributeContext = distributeContext;
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
     * 获取语义分析类
     * 该方法调用的入口发生在explaintask或者submittask中。
     * 在InsertUserOperatorTask中已经将CreateOperatorContext和InsertUserOperatorStatementContext
     * 两个类合并成了一个新类FullUserOperatorContext
     * 所以语义分析的时候不会调用CreateOperatorContext和InsertUserOperatorStatementContext的createAnalyzer接口
     * 只会调用FullUserOperatorContext中的方法
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
        walkExpression(walker, distributeContext);
    }
    
    public ParallelClauseContext getParallelNumber()
    {
        return parallelNumber;
    }
    
    public void setParallelNumber(ParallelClauseContext parallelNumber)
    {
        this.parallelNumber = parallelNumber;
    }
    
}
