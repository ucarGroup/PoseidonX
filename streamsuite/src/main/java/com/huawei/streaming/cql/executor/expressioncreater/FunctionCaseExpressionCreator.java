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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionCaseExpressionDesc;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.CaseSimpleExpression;
import com.huawei.streaming.expression.IExpression;

/**
 * case表达式实例创建
 * 
 */
public class FunctionCaseExpressionCreator implements ExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionCaseExpressionCreator.class);
    
    private FunctionCaseExpressionDesc expressiondesc;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IExpression createInstance(ExpressionDescribe expressionDescribe, Map<String, String> systemConfig)
        throws ExecutorException
    {
        LOG.info("start to create function case expression");
        expressiondesc = (FunctionCaseExpressionDesc)expressionDescribe;
        
        IExpression inputExpr =
            ExpressionCreatorFactory.createExpression(expressiondesc.getCasePropertyExpression(), systemConfig);
        
        List<Pair<IExpression, IExpression>> whenThenExprs = Lists.newArrayList();
        for (int i = 0; i < expressiondesc.getWhenThens().size(); i++)
        {
            ExpressionDescribe whenFirst = expressiondesc.getWhenThens().get(i).getFirst();
            ExpressionDescribe whenSecond = expressiondesc.getWhenThens().get(i).getSecond();
            IExpression whenExpression = ExpressionCreatorFactory.createExpression(whenFirst, systemConfig);
            IExpression thenExpression = ExpressionCreatorFactory.createExpression(whenSecond, systemConfig);
            whenThenExprs.add(new Pair<IExpression, IExpression>(whenExpression, thenExpression));
        }
        
        IExpression optionalElseExpr = null;
        if (expressiondesc.getElseExpression() != null)
        {
            optionalElseExpr =
                ExpressionCreatorFactory.createExpression(expressiondesc.getElseExpression(), systemConfig);
        }
        
        try
        {
            return new CaseSimpleExpression(inputExpr, whenThenExprs, optionalElseExpr);
        }
        catch (StreamingException e)
        {
            LOG.error("Can't create case simple expression.");
            throw ExecutorException.wrapStreamingException(e);
        }
    }
}
