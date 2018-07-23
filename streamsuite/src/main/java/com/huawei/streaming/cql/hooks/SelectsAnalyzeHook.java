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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectWithOutFromAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectStatementContext;

/**
 * select查询语句解析完成之后待执行的动作
 * 将输出的scheam加入到schmea的列表中，以待下一次使用
 * 
 */
public class SelectsAnalyzeHook implements SemanticAnalyzeHook
{
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        return parseContext instanceof SelectStatementContext;
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
        if (analyzeConext instanceof SelectAnalyzeContext)
        {
            SelectAnalyzeContext scontext = (SelectAnalyzeContext) analyzeConext;
            context.addSchema(scontext.getSelectClauseContext().getOutputSchema());
        } else if (analyzeConext instanceof SelectWithOutFromAnalyzeContext)
        {
            SelectWithOutFromAnalyzeContext scontext = (SelectWithOutFromAnalyzeContext) analyzeConext;
            context.addSchema(scontext.getSelectClauseContext().getOutputSchema());
        } else {
            return;
        }
    }
    
}
