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
import com.huawei.streaming.config.ConfVariable;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLResult;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzerFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SetAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * set命令执行器
 *
 */
public class SetTask extends BasicTask
{
    private static final Logger LOG = LoggerFactory.getLogger(SetTask.class);
    
    private DriverContext context;
    
    private SetAnalyzeContext setContext;
    
    private List<Schema> EMPTY_SCHEMAS = Collections.emptyList();

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
        LOG.debug("start to execute set task.");
        if (parseContext == null)
        {
            LOG.error("ParseContext is null.");
            throw new CQLException(ErrorCode.SEMANTICANALYZE_CONTEXT_NULL);
        }
        
        parseSet(parseContext);
        
        String value = null;
        try
        {
            value = ConfVariable.getValue(new ConfVariable(setContext.getValue()), getConf(), context.getUserConfs());
        }
        catch (StreamingException e)
        {
            LOG.error("Get config variable value error.");
            throw CQLException.wrapStreamingException(e);
        }
        context.addConf(setContext.getKey(), value);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CQLResult getResult()
    {
        return null;
    }
    
    private void parseSet(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, EMPTY_SCHEMAS);
        setContext = (SetAnalyzeContext)analyzer.analyze();
    }
}
