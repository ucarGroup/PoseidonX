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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc;

import java.util.List;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.opereators.JoinType;

/**
 * Join表达式描述
 * 
 */
public class JoinExpressionDesc implements ExpressionDescribe
{
    private JoinType jointype;
    
    private ExpressionDescribe leftExpression;
    
    private ExpressionDescribe rightExpression;
    
    private ExpressionDescribe joinCondition;
    
    /**
     * <默认构造函数>
     */
    public JoinExpressionDesc(JoinType type)
    {
        this.jointype = type;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return jointype.toString();
    }
    
    /**
     * 按照顺序，从左到右，获取所有join的流名称
     */
    public List<StreamAliasDesc> getOrderedStreams()
    {
        List<StreamAliasDesc> list = Lists.newArrayList();
        if (leftExpression instanceof JoinExpressionDesc)
        {
            list.addAll(((JoinExpressionDesc)leftExpression).getOrderedStreams());
        }
        else
        {
            list.add((StreamAliasDesc)leftExpression);
        }
        
        if (rightExpression instanceof JoinExpressionDesc)
        {
            list.addAll(((JoinExpressionDesc)rightExpression).getOrderedStreams());
        }
        else
        {
            list.add((StreamAliasDesc)rightExpression);
        }
        return list;
    }
    
    public JoinType getJointype()
    {
        return jointype;
    }
    
    public void setJointype(JoinType jointype)
    {
        this.jointype = jointype;
    }
    
    public ExpressionDescribe getLeftExpression()
    {
        return leftExpression;
    }
    
    public void setLeftExpression(ExpressionDescribe leftExpression)
    {
        this.leftExpression = leftExpression;
    }
    
    public ExpressionDescribe getRightExpression()
    {
        return rightExpression;
    }
    
    public void setRightExpression(ExpressionDescribe rightExpression)
    {
        this.rightExpression = rightExpression;
    }
    
    public ExpressionDescribe getJoinCondition()
    {
        return joinCondition;
    }
    
    public void setJoinCondition(ExpressionDescribe joinCondition)
    {
        this.joinCondition = joinCondition;
    }
}
