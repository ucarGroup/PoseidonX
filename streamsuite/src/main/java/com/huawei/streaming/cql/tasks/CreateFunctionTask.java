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
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.huawei.streaming.api.UserFunction;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLResult;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.CreateFunctionStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.KeyValuePropertyContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.StreamPropertiesContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * create function 命令执行器
 *
 */
public class CreateFunctionTask extends BasicTask
{
    private static final Logger LOG = LoggerFactory.getLogger(CreateFunctionTask.class);
    
    private DriverContext context;
    
    private CreateFunctionStatementContext functionContext;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void init(DriverContext driverContext, StreamingConfig config, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        super.init(driverContext, config, analyzeHooks);
        this.context = driverContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(ParseContext parseContext)
        throws CQLException
    {
        functionContext = (CreateFunctionStatementContext)parseContext;
        //所有的函数名称内部使用必须是小写的，但是在CQL中不区分大小写
        String functionName = functionContext.getFunctionName().toLowerCase(Locale.US);
        String functionClass = functionContext.getClassName();
        TreeMap<String, String> functionProperties = analyzeStreamProperties(functionContext.getFunctionProperties());
        LOG.info("create function {}", functionName);
        registerFunction(functionName, functionClass, functionProperties);
        context.addUserDefoundFunctions(createUserFunction(functionName, functionClass, functionProperties));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CQLResult getResult()
    {
        return null;
    }

    public UserFunction createUserFunction(String name, String clazz, TreeMap<String, String> properties)
    {
        UserFunction function = new UserFunction();
        function.setName(name);
        function.setClazz(clazz);
        function.setProperties(properties);
        return function;
    }

    private void registerFunction(String name, String clazz, Map<String, String> properties)
        throws CQLException
    {
        DriverContext.getFunctions().get().registerUDF(name, getFunctionClass(clazz), properties);
    }

    private Class< ? > getFunctionClass(String functionClass)
        throws ExecutorException
    {
        try
        {
            return Class.forName(functionClass, true, CQLUtils.getClassLoader());
        }
        catch (ClassNotFoundException e)
        {
            ExecutorException exception = new ExecutorException(ErrorCode.FUNCTION_UNSPPORTED, functionClass);
            LOG.error("Failed to get function class.", exception);
            throw exception;
        }
    }


    private TreeMap<String, String> analyzeStreamProperties(StreamPropertiesContext streamPropertiesContext)
    {
        if (streamPropertiesContext == null)
        {
            return Maps.newTreeMap();
        }

        return parseStreamProperties(streamPropertiesContext);
    }

    private TreeMap<String, String> parseStreamProperties(StreamPropertiesContext streamPropertiesContext)
    {
        TreeMap<String, String> properties = Maps.newTreeMap();
        for (KeyValuePropertyContext ctx : streamPropertiesContext.getProperties())
        {
            String key = ctx.getKey();
            String value = ctx.getValue();
            properties.put(key, value);
        }
        return properties;
    }

}
