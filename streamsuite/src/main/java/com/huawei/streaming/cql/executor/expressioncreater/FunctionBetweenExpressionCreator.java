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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionBetweenExpressionDesc;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.BetweenExpression;
import com.huawei.streaming.expression.ConstExpression;
import com.huawei.streaming.expression.IExpression;

/**
 * between表达式实例创建
 *
 */
public class FunctionBetweenExpressionCreator implements ExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionBetweenExpressionCreator.class);
    
    private FunctionBetweenExpressionDesc expressiondesc;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IExpression createInstance(ExpressionDescribe expressionDescribe, Map<String, String> systemConfig)
        throws ExecutorException
    {
        LOG.info("start to create function between expression");
        expressiondesc = (FunctionBetweenExpressionDesc)expressionDescribe;
        
        IExpression leftExpr =
            ExpressionCreatorFactory.createExpression(expressiondesc.getBetweenProperty(), systemConfig);
        IExpression lowExpr =
            ExpressionCreatorFactory.createExpression(expressiondesc.getLeftExpression(), systemConfig);
        IExpression highExpr =
            ExpressionCreatorFactory.createExpression(expressiondesc.getRightExpression(), systemConfig);
        
        validateArgExpressions(lowExpr, highExpr);
        
        /*
         * 同in，逻辑正好相反，boolean判断需要取反
         */
        try
        {
            return new BetweenExpression(leftExpr, (ConstExpression)lowExpr, (ConstExpression)highExpr,
                !expressiondesc.isContainsNotExpression());
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to create between expression.");
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    private void validateArgExpressions(IExpression lowExpr, IExpression highExpr)
        throws ExecutorException
    {
        if (!(lowExpr instanceof ConstExpression) || !(highExpr instanceof ConstExpression))
        {
            ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_IN_BETWEEN_EXPRESSION);
            LOG.error("Not const expression in between expression.", exception);
            throw exception;
        }
    }
}
