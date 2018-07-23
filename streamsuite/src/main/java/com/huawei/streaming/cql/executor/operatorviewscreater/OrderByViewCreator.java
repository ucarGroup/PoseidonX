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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.expressioncreater.ExpressionCreatorFactory;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzerFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.OrderByClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.process.sort.SortCondition;

/**
 * 创建orderbyview实例
 *
 */
public class OrderByViewCreator
{
    
    private static final Logger LOG = LoggerFactory.getLogger(OrderByViewCreator.class);
    
    /**
     * 创建groupby实例
     *
     */
    public List<SortCondition> create(List<Schema> uotputSchemas, String orderbyCondition)
        throws ExecutorException
    {
        if (StringUtils.isEmpty(orderbyCondition))
        {
            return null;
        }
        
        OrderByClauseAnalyzeContext analyzeContext = analyzeOrderByExpression(uotputSchemas, orderbyCondition);
        
        return ExpressionCreatorFactory.createOrderByExpression(analyzeContext);
        
    }
    
    private OrderByClauseAnalyzeContext analyzeOrderByExpression(List<Schema> uotputSchemas, String orderbyCondition)
        throws SemanticAnalyzerException
    {
        IParser parser = ParserFactory.createOrderbyClauseParser();
        SemanticAnalyzer analyzer =
            SemanticAnalyzerFactory.createAnalyzer(parser.parse(orderbyCondition), uotputSchemas);
        return (OrderByClauseAnalyzeContext)analyzer.analyze();
    }
    
}
