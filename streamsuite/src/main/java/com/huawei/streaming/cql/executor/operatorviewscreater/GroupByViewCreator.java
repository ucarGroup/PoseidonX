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
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzerFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.expression.IExpression;

/**
 * 创建groupby实例
 *
 */
public class GroupByViewCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(GroupByViewCreator.class);
    
    /**
     * 创建groupby实例
     *
     */
    public IExpression[] create(List<Schema> inputSchemas, String groupByExpress, Map<String, String> systemConfig)
        throws ExecutorException
    {
        
        if (StringUtils.isEmpty(groupByExpress))
        {
            LOG.info("group by expression is null.");
            return null;
        }
        
        SelectClauseAnalyzeContext analyzeContext = analyzeGroupByExpressions(inputSchemas, groupByExpress);
        
        List<ExpressionDescribe> exps = analyzeContext.getExpdes();
        IExpression[] expressions = new IExpression[exps.size()];
        for (int i = 0; i < exps.size(); i++)
        {
            expressions[i] = ExpressionCreatorFactory.createExpression(exps.get(i), systemConfig);
        }
        return expressions;
    }
    
    private SelectClauseAnalyzeContext analyzeGroupByExpressions(List<Schema> inputSchemas, String groupByExpress)
        throws SemanticAnalyzerException
    {
        IParser parser = ParserFactory.createGroupbyClauseParser();
        ParseContext parseContext = parser.parse(groupByExpress);
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, inputSchemas);
        return (SelectClauseAnalyzeContext)analyzer.analyze();
    }
    
}
