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
import com.huawei.streaming.cql.CQLResult;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FullUserOperatorContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.InsertUserOperatorStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * add file 命令执行器
 * 
 */
public class InsertUserOperatorTask extends BasicTask
{
    private static final Logger LOG = LoggerFactory.getLogger(InsertUserOperatorTask.class);
    
    private DriverContext context;
    
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
        InsertUserOperatorStatementContext operatorContext = (InsertUserOperatorStatementContext)parseContext;
        
        FullUserOperatorContext fullContext = new FullUserOperatorContext();
        fullContext.setInsertContext(operatorContext);
        String operatorName = operatorContext.getUsingContext().getOperatorName();
        if (!context.getUserOperators().containsKey(operatorName))
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_NO_USEROPERATOR, operatorName);
            LOG.error("Can not find user operator '" + operatorName + "'.", exception);
        }
        fullContext.setCreateContext(context.getUserOperators().get(operatorName));
        context.addParseContext(fullContext);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CQLResult getResult()
    {
        return null;
    }
}
