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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.BinaryExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.ArithmeticExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.LogicExpression;
import com.huawei.streaming.expression.RelationExpression;

/**
 * 二元表达式实例判断
 *
 */
public class BinaryExpressionCreator implements ExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(PropertyValueExpressionCreator.class);
    
    private BinaryExpressionDesc expressionDesc;
    
    private Map<String, String> applicationConfig;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IExpression createInstance(ExpressionDescribe expressionDescribe, Map<String, String> systemConfig)
        throws ExecutorException
    {
        LOG.info("start to create binary Expressions.");
        expressionDesc = (BinaryExpressionDesc)expressionDescribe;
        this.applicationConfig = systemConfig;
        
        switch (expressionDesc.getBexpression().getType())
        {
            case ADD:
            case SUBTRACT:
            case MULTIPLY:
            case DIVIDE:
            case MOD:
                return createArithmeticExpressionInstance();
            case LOGICAND:
            case LOGICOR:
                return createLogicExpressionInstance();
            default:
                return createRelationExpressionInstance();
        }
    }
    
    /**
     * 创建数学表达式
     * 比如a+b之类
     *
     */
    private IExpression createArithmeticExpressionInstance()
        throws ExecutorException
    {
        IExpression leftExpr =
            ExpressionCreatorFactory.createExpression(expressionDesc.getArgExpressions().get(0), applicationConfig);
        
        IExpression rightExpr =
            ExpressionCreatorFactory.createExpression(expressionDesc.getArgExpressions().get(1), applicationConfig);
        
        try
        {
            return new ArithmeticExpression(expressionDesc.getBexpression().getType(), leftExpr, rightExpr);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
        
    }
    
    /**
     * 创建关系表达式
     * 比如a>b之类
     *
     */
    private IExpression createRelationExpressionInstance()
        throws ExecutorException
    {
        IExpression leftExpr =
            ExpressionCreatorFactory.createExpression(expressionDesc.getArgExpressions().get(0), applicationConfig);
        IExpression rightExpr =
            ExpressionCreatorFactory.createExpression(expressionDesc.getArgExpressions().get(1), applicationConfig);
        
        try
        {
            return new RelationExpression(expressionDesc.getBexpression().getType(), leftExpr, rightExpr);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    /**
     * 创建逻辑表达式
     * 例如 a.id = b.id and a.name = c.cname
     *
     */
    private IExpression createLogicExpressionInstance()
        throws ExecutorException
    {
        IExpression leftExpr =
            ExpressionCreatorFactory.createExpression(expressionDesc.getArgExpressions().get(0), applicationConfig);
        IExpression rightExpr =
            ExpressionCreatorFactory.createExpression(expressionDesc.getArgExpressions().get(1), applicationConfig);
        
        try
        {
            return new LogicExpression(expressionDesc.getBexpression().getType(), leftExpr, rightExpr);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
        
    }
}
