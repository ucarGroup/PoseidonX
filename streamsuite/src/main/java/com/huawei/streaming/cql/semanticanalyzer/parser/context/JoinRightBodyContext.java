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

package com.huawei.streaming.cql.semanticanalyzer.parser.context;

import java.util.List;

import com.huawei.streaming.api.opereators.JoinType;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.Task;

/**
 * Join右侧内容解析结果
 * 
 */
public class JoinRightBodyContext extends ParseContext
{
    
    private JoinType joinType;
    
    private FromSourceContext rightStream;
    
    private OnConditionContext onCondition;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(" " + getJoinOperator());
        sb.append(" " + rightStream.toString());
        if (onCondition != null)
        {
            sb.append(" ON " + onCondition.toString());
        }
        
        return sb.toString();
    }
    
    private String getJoinOperator()
    {
        switch (joinType)
        {
            case LEFT_OUTER_JOIN:
                return "LEFT OUTER JOIN";
            case RIGHT_OUTER_JOIN:
                return "RIGHT OUTER JOIN";
            case FULL_OUTER_JOIN:
                return "FULL OUTER JOIN";
            case NATURAL_JOIN:
                return "NATURAL JOIN";
            case CROSS_JOIN:
                return "CROSS JOIN";
            default:
                return "INNER JOIN";
        }
    }
    
    public JoinType getJoinType()
    {
        return joinType;
    }
    
    public void setJoinType(JoinType joinType)
    {
        this.joinType = joinType;
    }
    
    public FromSourceContext getRightStream()
    {
        return rightStream;
    }
    
    public void setRightStream(FromSourceContext rightStream)
    {
        this.rightStream = rightStream;
    }
    
    public OnConditionContext getOnCondition()
    {
        return onCondition;
    }
    
    public void setOnCondition(OnConditionContext onCondition)
    {
        this.onCondition = onCondition;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, onCondition);
        walkExpression(walker, rightStream);
    }
    
}
