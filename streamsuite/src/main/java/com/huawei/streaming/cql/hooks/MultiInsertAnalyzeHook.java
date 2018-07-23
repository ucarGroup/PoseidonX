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


package com.huawei.streaming.cql.hooks;

import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertStatementAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.MultiInsertStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * multiinsert语句分析前后的钩子
 * 
 */
public class MultiInsertAnalyzeHook implements SemanticAnalyzeHook
{
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        return parseContext instanceof MultiInsertStatementContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void preAnalyze(DriverContext context, ParseContext parseContext)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void postAnalyze(DriverContext context, AnalyzeContext analyzeConext)
        throws SemanticAnalyzerException
    {
        MultiInsertStatementAnalyzeContext multiInsertContext = (MultiInsertStatementAnalyzeContext)analyzeConext;
        for(MultiInsertAnalyzeContext insertContext : multiInsertContext.getMultiSelectBodyAnalyzeContexts())
        {
            new SelectsAnalyzeHook().postAnalyze(context, insertContext.getSelectContext());
        }
    }
}
