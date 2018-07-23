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

import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.Application;
import com.huawei.streaming.application.ApplicationFactory;
import com.huawei.streaming.application.ApplicationResults;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLResult;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzerFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.ShowApplicationAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * 显示当前应用程序信息
 *
 */
public class ShowApplicationTask extends BasicTask
{
    private static final Logger LOG = LoggerFactory.getLogger(ShowApplicationTask.class);
    
    /**
     * 默认的应用程序名称，仅仅作为参数传入，并不会作为应用程序的名称导入
     */
    private static final String DEFAULT_APP_NAME = "default";
    
    private ShowApplicationAnalyzeContext analyzeContext;
    
    private ApplicationResults results = null;
    
    private List<Schema> EMPTY_SCHEMAS = Collections.emptyList();

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
        
        parseCommand(parseContext);
        getRemoteApplications();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CQLResult getResult()
    {
        CQLResult result = new CQLResult();
        result.setHeads(results.getResultHeader());
        result.setResults(results.getResults(analyzeContext.getApplicationName()));
        result.setFormatter(results.getFormatter());
        return result;
    }
    
    private void getRemoteApplications()
        throws ExecutorException
    {
        try
        {
            Application app = ApplicationFactory.createApplication(getDriverContext(),getConf(), DEFAULT_APP_NAME);
            results = app.getApplications();
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    private void parseCommand(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, EMPTY_SCHEMAS);
        analyzeContext = (ShowApplicationAnalyzeContext)analyzer.analyze();
    }
}
