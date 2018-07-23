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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionInExpressionDesc;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.ConstExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.InExpression;

/**
 * in表达式创建
 * 
 */
public class FunctionInExpressionCreator implements ExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionInExpressionCreator.class);
    
    private FunctionInExpressionDesc expressiondesc;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IExpression createInstance(ExpressionDescribe expressionDescribe, Map<String, String> systemConfig)
        throws ExecutorException
    {
        LOG.info("start to create function in expression");
        expressiondesc = (FunctionInExpressionDesc)expressionDescribe;
        
        IExpression leftExpr = ExpressionCreatorFactory.createExpression(expressiondesc.getInProperty(), systemConfig);
        
        ConstExpression[] rightExprs = new ConstExpression[expressiondesc.getArgs().size()];
        for (int i = 0; i < expressiondesc.getArgs().size(); i++)
        {
            IExpression exp = ExpressionCreatorFactory.createExpression(expressiondesc.getArgs().get(i), systemConfig);
            if (exp instanceof ConstExpression)
            {
                rightExprs[i] = (ConstExpression)exp;
            }
            else
            {
                ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_IN_BETWEEN_EXPRESSION);
                LOG.error("Not const expression in 'in' right expression.", exception);
                throw exception;
            }
        }
        
        /*
         * 这里的isIn逻辑正好和CQL部分相反
         * 布尔运算取反
         */
        try
        {
            return new InExpression(leftExpr, rightExprs, !expressiondesc.isContainsNotExpression());
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
        
    }
}
