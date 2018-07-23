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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 创建各类执行任务的工厂类
 *
 */
public class TaskFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(TaskFactory.class);
    
    /**
     * 创建task实例
     *
     */
    public static Task createTask(DriverContext driverContext, ParseContext parseContext, StreamingConfig config,
        List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        Task task = parseContext.createTask(driverContext, analyzeHooks);
        if (task == null)
        {
            LOG.error("Can not find task excutor for parse context '{}'.", parseContext.getClass().getName());
            CQLException exception = new CQLException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        task.init(driverContext, config, analyzeHooks);
        return task;
    }
}
