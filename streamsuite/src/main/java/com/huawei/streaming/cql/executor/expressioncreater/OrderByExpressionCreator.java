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

package com.huawei.streaming.cql.executor.expressioncreater;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.OrderByClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.process.sort.SortCondition;
import com.huawei.streaming.process.sort.SortEnum;

/**
 * 创建orderby 表达式
 *
 */
public class OrderByExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(OrderByExpressionCreator.class);
    
    /**
     * 创建orderby表达式解析实例
     *
     */
    public List<SortCondition> createInstance(OrderByClauseAnalyzeContext parseContext)
        throws ExecutorException
    {
        List<SortCondition> orderbys = new ArrayList<SortCondition>();
        List<Pair<ExpressionDescribe, SortEnum>> orderbyexpressions = parseContext.getOrderbyExpressions();
        
        for (int i = 0; i < orderbyexpressions.size(); i++)
        {
            ExpressionDescribe desc = orderbyexpressions.get(i).getFirst();
            SortEnum st = orderbyexpressions.get(i).getSecond();
            if (!(desc instanceof PropertyValueExpressionDesc))
            {
                ExecutorException exception =
                    new ExecutorException(ErrorCode.SEMANTICANALYZE_ORDERBY_EXPRESSION_UNSPPORTED);
                LOG.error("Not property expression in orderby clause.", exception);
                throw exception;
            }
            
            orderbys.add(new SortCondition(((PropertyValueExpressionDesc)desc).getProperty(), st));
        }
        return orderbys;
    }
    
}
