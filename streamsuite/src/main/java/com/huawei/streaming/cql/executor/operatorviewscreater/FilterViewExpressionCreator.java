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

package com.huawei.streaming.cql.executor.operatorviewscreater;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.expressioncreater.ExpressionCreatorFactory;
import com.huawei.streaming.cql.semanticanalyzer.FilterClauseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FilterClauseAnalzyeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.expression.IExpression;

/**
 * 创建filterview实例
 *
 */
public class FilterViewExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(FilterViewExpressionCreator.class);
    
    /**
     * filter实例创建
     *
     */
    public IExpression create(List<Schema> inputSchemas, String filterCondition, Map<String, String> systemConfig)
        throws ExecutorException
    {
        if (StringUtils.isEmpty(filterCondition))
        {
            return null;
        }
        
        FilterClauseAnalzyeContext analyzeContext = analzyeFilterExpression(inputSchemas, filterCondition);
        List<ExpressionDescribe> exprs = analyzeContext.getExpdes();
        return ExpressionCreatorFactory.createExpression(exprs.get(0), systemConfig);
        
    }
    
    private FilterClauseAnalzyeContext analzyeFilterExpression(List<Schema> inputSchemas, String filterCondition)
        throws SemanticAnalyzerException
    {
        IParser parser = ParserFactory.createApplicationParser();
        ParseContext parseContext = parser.parse(filterCondition);
        FilterClauseAnalyzer analyzer = new FilterClauseAnalyzer(parseContext);
        analyzer.init(inputSchemas);
        return (FilterClauseAnalzyeContext)analyzer.analyze();
    }
}
