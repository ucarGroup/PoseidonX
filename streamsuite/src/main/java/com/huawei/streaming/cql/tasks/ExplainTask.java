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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLResult;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.builder.ApplicationBuilder;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.PhysicalPlanWriter;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzerFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.ExplainApplicationAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.StreamingRuntimeException;

/**
 * explain命令解析
 * 
 */
public class ExplainTask extends BasicTask
{
    private static final Logger LOG = LoggerFactory.getLogger(ExplainTask.class);
    
    private DriverContext context;
    
    private List<AnalyzeContext> analyzeContexts;
    
    private ExplainApplicationAnalyzeContext explainContext;
    
    private Application resultApp = null;
    
    private String[] resultHeader = {"stringApp"};
    
    private List<Schema> EMPTY_SCHEMAS = Collections.emptyList();
    
    /**
     * 超过500会自动拓展
     */
    private String format = "%-500s";
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void init(DriverContext driverContext, StreamingConfig config, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        super.init(driverContext, config, analyzeHooks);
        context = driverContext;
        analyzeContexts = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(ParseContext parseContext)
        throws CQLException
    {
        parseExplain(parseContext);
        Application app = createApplication();
        exportApplication(app);
    }
    
    private Application createApplication()
        throws CQLException
    {
        if (context.getApp() == null)
        {
            return buildApplication();
        }
        
        return context.getApp();
    }
    
    private Application buildApplication()
        throws CQLException
    {
        Application app;
        analyzeApplicationContexts();
        String appName = explainContext.getAppName();
        app = new ApplicationBuilder().build(appName, analyzeContexts, context);
        return app;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CQLResult getResult()
    {
        if (null != resultApp)
        {
            CQLResult result = new CQLResult();
            result.setHeads(resultHeader);
            result.setResults(createResults());
            result.setFormatter(format);
            return result;
        }
        return null;
    }
    
    private List<String[]> createResults()
    {
        List<String[]> res = Lists.newArrayList();
        String[] r = {createStringApp()};
        res.add(r);
        return res;
    }
    
    private void parseExplain(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, EMPTY_SCHEMAS);
        explainContext = (ExplainApplicationAnalyzeContext)analyzer.analyze();
    }
    
    private String createStringApp()
    {
        return PhysicalPlanWriter.createStringPlan(resultApp);
    }
    
    private void exportApplication(Application app)
    {
        LOG.info("start to export application " + app.getApplicationId());
        if (!StringUtils.isEmpty(explainContext.getSavePath()))
        {
            PhysicalPlanWriter.write(app, explainContext.getSavePath());
        }
        else
        {
            resultApp = app;
        }
    }
    
    private void analyzeApplicationContexts()
        throws SemanticAnalyzerException
    {
        try
        {
            for (ParseContext parseContext : context.getParseContexts())
            {
                preAnalyze(context, parseContext);
                SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, context.getSchemas());
                AnalyzeContext analyzeResult = analyzer.analyze();
                analyzeContexts.add(analyzeResult);
                postAnalyze(context, analyzeResult, parseContext);
            }
        }
        catch (StreamingRuntimeException e)
        {
            //防止表达式创建或者算子实例创建的时候抛出runtime异常
            //不过这里只捕获StreamingRuntimeException异常
            //RuntimeException计划后期全部替换成StreamingRuntimeException
            throw SemanticAnalyzerException.wrapStreamingRunTimeException(e);
        }
    }
}
