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

import com.huawei.streaming.cql.semanticanalyzer.parser.context.AddFileStatementContext;
import com.ucar.streamsuite.common.constant.StreamContant;
import com.ucar.streamsuite.engine.constants.EngineContant;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.PhysicalPlan;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.ApplicationFactory;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLResult;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.builder.ApplicationBuilder;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.PhysicalPlanExecutor;
import com.huawei.streaming.cql.executor.PhysicalPlanLoader;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzerFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SubmitApplicationAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;

/**
 * 提交应用程序执行task
 *
 */
public class SubmitTask extends BasicTask
{
    private static final Logger LOG = LoggerFactory.getLogger(SubmitTask.class);

    private DriverContext context;

    private List<AnalyzeContext> analyzeContexts;

    private SubmitApplicationAnalyzeContext submitContext;

    private String[] resultHeader = {"result"};

    private String result = null;

    private List<Schema> emptySchemas = Collections.emptyList();

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
        if (parseContext == null)
        {
            LOG.error("ParseContext is null.");
            throw new CQLException(ErrorCode.SEMANTICANALYZE_CONTEXT_NULL);
        }

        securityPrepare();
        parseSubmit(parseContext);


        createApplication();

        LOG.error("########### JstormConstant.RTCP_CQL_IS_SUBMIT #############:"+context.getCqlClient().getCustomizedConfigurationMap().get(EngineContant.RTCP_CQL_IS_SUBMIT));

        if(context.getCqlClient().getCustomizedConfigurationMap().get(EngineContant.RTCP_CQL_IS_SUBMIT) != null
             && Boolean.valueOf(String.valueOf(context.getCqlClient().getCustomizedConfigurationMap().get(EngineContant.RTCP_CQL_IS_SUBMIT))).equals(false)){
            LOG.error("不提交任务,这是做cql语法校验");
            return;
        }

        dropApplicationIfAllow();
        submitApplication();
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

    /**
     *  安全准备
     *  1、检查有没有设置streaming.security.keytab.path参数和streaming.security.user.principal参数
     *   检查的时候，不论default还是streaming-site还是用户自定义参数，都需要进行检查
     *  2、执行add file添加keytab文件
     */
    private void securityPrepare()
        throws CQLException
    {
        if (getConf().containsKey(StreamingConfig.STREAMING_SECURITY_KEYTAB_PATH))
        {
            if (getConf().containsKey(StreamingConfig.STREAMING_SECURITY_USER_PRINCIPAL))
            {
                AddFileStatementContext addFileContext = new AddFileStatementContext();
                try
                {
                    addFileContext.setPath(getConf().getStringValue(StreamingConfig.STREAMING_SECURITY_KEYTAB_PATH));
                    AddFileTask task = new AddFileTask();
                    task.init(context, getConf(), getAnalyzeHooks());
                    task.execute(addFileContext);
                }
                catch (StreamingException e)
                {
                    throw CQLException.wrapStreamingException(e);
                }
            }
        }
    }

    private void dropApplicationIfAllow()
        throws CQLException
    {
        if (!Boolean.valueOf((String)getConf().get(StreamingConfig.STREAMING_STORM_SUBMIT_ISLOCAL)))
        {
            if (checkApplicationExists())
            {
                dropApplication();
            }
        }
    }

    private boolean checkApplicationExists()
        throws ExecutorException
    {
        try
        {
            com.huawei.streaming.application.Application app =
                ApplicationFactory.createApplication(getDriverContext(),getConf(), submitContext.getAppName());
            return app.isApplicationExists();
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }

    private List<String[]> createResults()
    {
        List<String[]> res = Lists.newArrayList();
        String[] r = {result};
        res.add(r);
        return res;
    }

    private void parseSubmit(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, emptySchemas);
        submitContext = (SubmitApplicationAnalyzeContext)analyzer.analyze();
    }

    private void submitApplication()
        throws CQLException
    {
        new PhysicalPlanExecutor().execute(context.getApp(),getDriverContext());
        result = "Application " + context.getApp().getApplicationName() + " submitted successfully.";
    }

    private void dropApplication()
        throws CQLException
    {
        if (!submitContext.isForceSubmit())
        {
            result = "Application " + submitContext.getAppName() + " is already exists.";
            return;
        }

        DropApplicationTask task = new DropApplicationTask();
        task.init(context, getConf(), super.getAnalyzeHooks());
        task.dropApplication(submitContext.getAppName(), true);
    }

    private void createApplication()
        throws CQLException
    {
        String savePath = submitContext.getPhysicPlanPath();
        String appName = submitContext.getAppName();
        if (!StringUtils.isEmpty(savePath))
        {
            PhysicalPlan plan = PhysicalPlanLoader.load(savePath);
            Application app = plan.getApploication();
            app.setApplicationId(appName);
            context.setApp(app);
            return;
        }

        context.setApp(createAPIApplication(appName));
    }

    private Application createAPIApplication(String appName)
        throws CQLException
    {
        checkApplicationNameIsEmpty(appName);
        Application app = null;
        if (context.getApp() == null)
        {
            semanticAnalyzerLazyContexts();
            app = new ApplicationBuilder().build(appName, analyzeContexts, context);
        }
        else
        {
            app = context.getApp();
        }

        app.setApplicationId(appName);
        return app;
    }

    private void checkApplicationNameIsEmpty(String appName)
        throws ExecutorException
    {
        if (StringUtils.isEmpty(appName))
        {
            ExecutorException exception = new ExecutorException(ErrorCode.TOP_NO_NAME);
            LOG.error("Application name is null.", exception);
            throw exception;
        }
    }

    private void semanticAnalyzerLazyContexts()
        throws SemanticAnalyzerException
    {
        try
        {
            for (ParseContext parseContext : context.getParseContexts())
            {
                preAnalyze(context, parseContext);
                SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, context.getSchemas());
                AnalyzeContext analyzeContext = analyzer.analyze();
                postAnalyze(context, analyzeContext, parseContext);
                analyzeContexts.add(analyzeContext);
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
