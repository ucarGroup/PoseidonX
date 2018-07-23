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

package com.huawei.streaming.cql.tasks;

import java.util.List;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLResult;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * 各类命令执行接口
 * 
 */
public interface Task
{
    /**
     * 初始化
     */
    void init(DriverContext driverContext, StreamingConfig config, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException;
    
    /**
     * 任务执行接口
     * 由于LazyTask的存在，部分任务是延迟处理的，
     * 所以该接口返回值为空，另外设置getResult接口获取task执行结果
     */
    void execute(ParseContext parseContext)
        throws CQLException;
    
    /**
     * 获取解析结果
     */
    CQLResult getResult();
    
    /**
     * 语义分析之前执行的动作
     */
    void preAnalyze(DriverContext context, ParseContext parseContext)
        throws SemanticAnalyzerException;
    
    /**
     * 语义分析之后执行的动作
     */
    void postAnalyze(DriverContext context, AnalyzeContext analyzeConext, ParseContext parseContext)
        throws SemanticAnalyzerException;
    
}
