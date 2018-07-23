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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.RebalanceApplicationAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * Rebalance应用程序
 */
public class RebalanceTask extends BasicTask
{
    
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceTask.class);
    
    private RebalanceApplicationAnalyzeContext analyzeContext;
    
    private String[] resultHeader = {"result"};
    
    private String result = null;
    
    private List<Schema> emptySchemas = Collections.emptyList();
    
    private int workerNum;
    
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
        if (null == parseContext)
        {
            LOG.error("ParseContext is null");
            throw new CQLException(ErrorCode.SEMANTICANALYZE_CONTEXT_NULL);
        }
        parseCommond(parseContext);
        
        getWorkerNumAndCheck();
        
        rebalanceApplication();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CQLResult getResult()
    {
        CQLResult res = new CQLResult();
        res.setHeads(resultHeader);
        res.setResults(createResults());
        res.setFormatter(format);
        return res;
    }
    
    private List<String[]> createResults()
    {
        List<String[]> res = Lists.newArrayList();
        String[] r = {result};
        res.add(r);
        return res;
    }
    
    private void parseCommond(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, emptySchemas);
        analyzeContext = (RebalanceApplicationAnalyzeContext)analyzer.analyze();
    }
    
    private void getWorkerNumAndCheck()
        throws CQLException
    {
        workerNum = Integer.valueOf(analyzeContext.getWorkerNum());
        if (workerNum == 0)
        {
            LOG.error("Invalid workerNumber");
            throw new CQLException(ErrorCode.CONFIG_VALUE_ERROR,analyzeContext.getWorkerNum());
        }
    }
    
    private void rebalanceApplication()
        throws ExecutorException
    {
        String appName = analyzeContext.getApplicationName();
        try
        {
            Application app = ApplicationFactory.createApplication(getDriverContext(),getConf(), analyzeContext.getApplicationName());
            app.rebalanceApplication(workerNum);
            result = "The rebalance request for the application " + appName + " is submitted successfully.";
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    

    
}
