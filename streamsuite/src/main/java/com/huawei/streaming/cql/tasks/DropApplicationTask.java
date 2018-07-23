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

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.Application;
import com.huawei.streaming.application.ApplicationFactory;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLResult;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzerFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.DropApplicationAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * drop appliation 命令执行器
 *
 */
public class DropApplicationTask extends BasicTask
{
    private static final Logger LOG = LoggerFactory.getLogger(DropApplicationTask.class);
    
    private DropApplicationAnalyzeContext dropContext;
    
    private String[] resultHeader = {"result"};
    
    private String result = null;
    
    private List<Schema> EMPTY_SCHEMAS = Collections.emptyList();
    
    /**
     * 超过会自动拓展
     */
    private String format = "%-20s";
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void init(DriverContext driverContext, StreamingConfig config, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        super.init(driverContext, config, analyzeHooks);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(ParseContext parseContext)
        throws CQLException
    {
        if (parseContext == null)
        {
            LOG.error("ParseContext is null.");
            throw new CQLException(ErrorCode.SEMANTICANALYZE_CONTEXT_NULL);
        }
        
        parseDrop(parseContext);
        drop();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CQLResult getResult()
    {
        CQLResult rs = new CQLResult();
        rs.setHeads(resultHeader);
        rs.setResults(createResults());
        rs.setFormatter(format);
        return rs;
        
    }
    
    private List<String[]> createResults()
    {
        List<String[]> res = Lists.newArrayList();
        String[] r = {result};
        res.add(r);
        return res;
    }
    
    /**
     * 删除应用程序
     */
    protected void dropApplication(String appName, boolean isCheckExist)
        throws ExecutorException
    {
        dropContext = new DropApplicationAnalyzeContext();
        dropContext.setAppName(appName);
        dropContext.setCheckExist(isCheckExist);
        drop();
    }
    
    private void drop()
        throws ExecutorException
    {
        try
        {
            Application app = ApplicationFactory.createApplication(getDriverContext(),getConf(), dropContext.getAppName());
            if (dropContext.isCheckExist())
            {
                if (!app.isApplicationExists())
                {
                    result = "Application " + dropContext.getAppName() + " does not exist.";
                    return;
                }
            }
            app.killApplication();
            result = "Application " + dropContext.getAppName() + " killed successfully.";
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    private void parseDrop(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, EMPTY_SCHEMAS);
        dropContext = (DropApplicationAnalyzeContext)analyzer.analyze();
    }
}
