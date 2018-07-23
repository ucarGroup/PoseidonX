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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.process.sort.SortEnum;

/**
 * orderby子句语义分析内容
 * 
 */
public class OrderByClauseAnalyzeContext extends AnalyzeContext
{
    /**
     * orderby表达式信息
     */
    private List<Pair<ExpressionDescribe, SortEnum>> orderbyExpressions =
        new ArrayList<Pair<ExpressionDescribe, SortEnum>>();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<Schema> getCreatedSchemas()
    {
        return Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParseContext(ParseContext parseContext)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void validateParseContext()
        throws SemanticAnalyzerException
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < orderbyExpressions.size(); i++)
        {
            Pair<ExpressionDescribe, SortEnum> p = orderbyExpressions.get(i);
            sb.append(" " + p.getFirst().toString() + " " + p.getSecond().getDesc());
            if (i != orderbyExpressions.size() - 1)
            {
                sb.append(",");
            }
        }
        
        return sb.toString();
    }
    
    /**
     * 添加表达式
     */
    public void addOrderByExpression(ExpressionDescribe expression, SortEnum type)
    {
        orderbyExpressions.add(new Pair<ExpressionDescribe, SortEnum>(expression, type));
    }
    
    public List<Pair<ExpressionDescribe, SortEnum>> getOrderbyExpressions()
    {
        return orderbyExpressions;
    }
    
}
