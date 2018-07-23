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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.DeactiveApplicationAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * Deactive应用程序
 *
 */
public class DeactiveTask extends BasicTask
{
    private static final Logger LOG = LoggerFactory.getLogger(DeactiveTask.class);
    
    private DeactiveApplicationAnalyzeContext analyzeContext;

    private List<Schema> emptySchemas = Collections.emptyList();

    private String[] resultHeader = {"result"};

    private String result = null;

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
        
        parseCommand(parseContext);
        deactiveApplication();
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

    private void deactiveApplication()
        throws ExecutorException
    {
        try
        {
            Application app = ApplicationFactory.createApplication(getDriverContext(),getConf(), analyzeContext.getApplicationName());
            app.deactiveApplication();
            result = "Application " + analyzeContext.getApplicationName() + " deactivated successfully.";
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    private void parseCommand(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, emptySchemas);
        analyzeContext = (DeactiveApplicationAnalyzeContext)analyzer.analyze();
    }
}
