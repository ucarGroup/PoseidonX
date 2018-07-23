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
 * 解析内容的抽象类，用来注册一些解析类的公共方法
 * 
 * 为了避免toString方法的遗忘，所以将本类从接口改为抽象类.
 * 如果是接口的话，不写toString方法，也不会导致报错
 * 
 */
public abstract class ParseContext
{
    /**
     * {@inheritDoc}
     */
    @Override
    public abstract String toString();
    
    /**
     * 创建对应语句的执行task
     */
    public abstract Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException;
    
    /**
     * 创建语义分析执行解析器
     */
    public abstract SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException;
    
    /**
     * 遍历自身以及子表达式
     */
    public void walk(ParseContextWalker walker)
    {
        if (!walker.walk(this))
        {
            walkChild(walker);
        }
    }
    
    /**
     * 遍历表达式
     */
    protected void walkExpression(ParseContextWalker walker, ParseContext parseContext)
    {
        if (parseContext == null)
        {
            return;
        }
        
        if (!walker.walk(parseContext))
        {
            parseContext.walk(walker);
        }
    }
    
    /**
     * 遍历表达式列表
     */
    protected void walkExpressions(ParseContextWalker walker, List<BaseExpressionParseContext> expressions)
    {
        for (BaseExpressionParseContext child : expressions)
        {
            walkExpression(walker, child);
        }
    }
    
    /**
     * 遍历子节点的时候，一定要保证每个子节点都遍历到，不能因为一个已经匹配，就不遍历其他节点
     */
    protected abstract void walkChild(ParseContextWalker walker);
}
