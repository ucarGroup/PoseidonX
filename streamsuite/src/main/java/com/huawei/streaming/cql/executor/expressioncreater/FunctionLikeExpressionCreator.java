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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionLikeExpressionDesc;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.ConstExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.LikeExpression;

/**
 * like表达式实例创建
 *
 */
public class FunctionLikeExpressionCreator implements ExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionLikeExpressionCreator.class);
    
    private FunctionLikeExpressionDesc expressiondesc;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IExpression createInstance(ExpressionDescribe expressionDescribe, Map<String, String> systemConfig)
        throws ExecutorException
    {
        LOG.info("start to create function like expression");
        expressiondesc = (FunctionLikeExpressionDesc)expressionDescribe;
        
        IExpression leftExpr =
            ExpressionCreatorFactory.createExpression(expressiondesc.getLikeProperty(), systemConfig);
        IExpression strExp =
            ExpressionCreatorFactory.createExpression(expressiondesc.getLikeStringExpression(), systemConfig);
        if (!(strExp instanceof ConstExpression))
        {
            ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_LIKE_STRING);
            LOG.error("Not const expression in Like right expression.", exception);
            throw exception;
        }
        
        ConstExpression constrExp = (ConstExpression)strExp;
        if (String.class != constrExp.getType())
        {
            ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_LIKE_STRING);
            LOG.error("Not string type in Like right expression.", exception);
            throw exception;
        }
        
        /*
         * 同in，逻辑正好相反，boolean判断需要取反
         */
        try
        {
            return new LikeExpression(leftExpr, constrExp.getConstValue().toString(),
                !expressiondesc.isContainsNotExpression());
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
}
