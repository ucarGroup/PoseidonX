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
import com.huawei.streaming.cql.semanticanalyzer.RebalanceApplicationAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.RebalanceTask;
import com.huawei.streaming.cql.tasks.Task;

/**
 * Rebalance应用程序解析内容
 */
public class RebalanceApplicationContext extends ParseContext
{
    
    private String applicationName;
    
    private String workerNumber;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("REBALANCE APPLICATION ");
        sb.append(applicationName);
        sb.append("SET WORKER ");
        sb.append(workerNumber);
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        return new RebalanceTask();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException
    {
        return new RebalanceApplicationAnalyzer(this);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        
    }
    
    public String getApplicationName()
    {
        return applicationName;
    }
    
    public void setApplicationName(String applicationName)
    {
        this.applicationName = applicationName;
    }
    
    public String getApplicationWorkerNum()
    {
        return workerNumber;
    }
    
    public void setApplicationWorkerNum(String num)
    {
        this.workerNumber = num;
    }
}
