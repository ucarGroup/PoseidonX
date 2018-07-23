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
 * JoinSource语法解析内容
 * 
 */
public class JoinSourceContext extends ParseContext
{
    private List<JoinRightBodyContext> joinRightBody;
    
    /*
     * 如果存在Join，该字段就是最左边的join流
     * 如果不存在join，该字段就是唯一的数据源流
     */
    private FromSourceContext leftStream;
    
    /**
     * <默认构造函数>
     */
    public JoinSourceContext()
    {
        joinRightBody = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return leftStream.toString() + Joiner.on(" ").join(joinRightBody);
    }
    
    public FromSourceContext getLeftStream()
    {
        return leftStream;
    }
    
    public void setLeftStream(FromSourceContext leftStream)
    {
        this.leftStream = leftStream;
    }
    
    public List<JoinRightBodyContext> getJoinRightBody()
    {
        return joinRightBody;
    }
    
    public void setJoinRightBody(List<JoinRightBodyContext> joinRightBody)
    {
        this.joinRightBody = joinRightBody;
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
        walkExpression(walker, leftStream);
        for (JoinRightBodyContext right : joinRightBody)
        {
            walkExpression(walker, right);
        }
    }
    
}
