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

import java.util.List;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FilterClauseAnalzyeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * filter子句语法解析
 * 
 */
public class FilterClauseAnalyzer extends ClauseAfterAggregateAnalyzer
{
    private FilterClauseAnalzyeContext analyzeContext;
    
    private ExpressionContext expressionContext;
    
    /**
     * <默认构造函数>
     */
    public FilterClauseAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        expressionContext = (ExpressionContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        List<Schema> parseSchemas = getAllSchemas();
        
        /*
         * 如果属于having子句
         * 替换字句中属于原始输入schema的列
         * 如果子句中的列在输入和输出中都包含，则使用输出schema的作为标准
         */
        if (getSelectItems() != null)
        {
            replaceInputSchemas(expressionContext);
            parseSchemas = Lists.newArrayList(getOutputSchema());
        }
        
        ExpressionDescribe exp = ExpressionDescFactory.createExpressionDesc(expressionContext, parseSchemas);
        if (null != exp)
        {
            analyzeContext.addExpressionDesc(exp);
        }
        
        return analyzeContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        analyzeContext = new FilterClauseAnalzyeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return analyzeContext;
    }
    
}
