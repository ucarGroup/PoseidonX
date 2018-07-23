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

package com.huawei.streaming.cql.builder.operatorsplitter;

import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.InsertAnalyzeContext;

/**
 * insert into 语句的拆分
 * 
 */
public class InsertSplitter implements Splitter
{
    private BuilderUtils buildUtils;
    
    private SplitContext result = new SplitContext();
    
    private InsertAnalyzeContext context;
    
    /**
     * <默认构造函数>
     */
    public InsertSplitter(BuilderUtils buildUtils)
    {
        this.buildUtils = buildUtils;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(AnalyzeContext parseContext)
    {
        return parseContext instanceof InsertAnalyzeContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SplitContext split(AnalyzeContext parseContext)
        throws ApplicationBuildException
    {
        context = (InsertAnalyzeContext)parseContext;
        result.setOutputStreamName(context.getOutputStreamName());
        SplitContext selectResult = OperatorSplitter.split(buildUtils, context.getSelectContext());
        result.getOperators().addAll(selectResult.getOperators());
        result.getTransitions().addAll(selectResult.getTransitions());
        result.setParseContext(context);
        return result;
    }
}
