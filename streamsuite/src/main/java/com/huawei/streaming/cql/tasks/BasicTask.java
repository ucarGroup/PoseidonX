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
 * 基础任务执行器
 *
 */
public abstract class BasicTask implements Task
{
    private List< SemanticAnalyzeHook > hooks;

    private StreamingConfig conf;

    private DriverContext driverContext;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(DriverContext driverContext, StreamingConfig config, List< SemanticAnalyzeHook > analyzeHooks)
     throws CQLException
    {
        this.hooks = analyzeHooks;
        this.conf = config;
        this.driverContext = driverContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void execute(ParseContext parseContext)
     throws CQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract CQLResult getResult();

    /**
     * {@inheritDoc}
     */
    @Override
    public void preAnalyze(DriverContext cxt, ParseContext parseContext)
     throws SemanticAnalyzerException
    {
        for (int i = 0; i < hooks.size(); i++)
        {
            if (hooks.get(i).validate(parseContext))
            {
                hooks.get(i).preAnalyze(cxt, parseContext);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void postAnalyze(DriverContext context, AnalyzeContext analyzeConext, ParseContext parseContext)
     throws SemanticAnalyzerException
    {
        for (int i = 0; i < hooks.size(); i++)
        {
            if (hooks.get(i).validate(parseContext))
            {
                hooks.get(i).postAnalyze(context, analyzeConext);
            }
        }
    }

    public List< SemanticAnalyzeHook > getAnalyzeHooks()
    {
        return hooks;
    }

    public StreamingConfig getConf()
    {
        return conf;
    }

    public DriverContext getDriverContext() {
        return driverContext;
    }
}
