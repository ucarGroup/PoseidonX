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

package com.huawei.streaming.cql.semanticanalyzer;

import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.ShowApplicationAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * 
 * 显示应用程序
 * 
 */
public class ShowApplicationAnalyzer extends BaseAnalyzer
{
    private ShowApplicationAnalyzeContext context;
    
    /**
     * <默认构造函数>
     */
    public ShowApplicationAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        context = new ShowApplicationAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return context;
    }
    
}
