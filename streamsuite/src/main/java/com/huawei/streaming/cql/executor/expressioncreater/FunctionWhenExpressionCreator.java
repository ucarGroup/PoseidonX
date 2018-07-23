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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionWhenExpressionDesc;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.CaseSearchedExpression;
import com.huawei.streaming.expression.IExpression;

/**
 * When表达式实例创建
 *
 */
public class FunctionWhenExpressionCreator implements ExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionWhenExpressionCreator.class);
    
    private FunctionWhenExpressionDesc expressiondesc;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IExpression createInstance(ExpressionDescribe expressionDescribe, Map<String, String> systemConfig)
        throws ExecutorException
    {
        LOG.info("start to create function when expression");
        expressiondesc = (FunctionWhenExpressionDesc)expressionDescribe;
        
        List<Pair<IExpression, IExpression>> whenThenExprs = new ArrayList<Pair<IExpression, IExpression>>();
        for (int i = 0; i < expressiondesc.getWhenThens().size(); i++)
        {
            ExpressionDescribe expFirst = expressiondesc.getWhenThens().get(i).getFirst();
            ExpressionDescribe expSecond = expressiondesc.getWhenThens().get(i).getSecond();
            IExpression whenExpression = ExpressionCreatorFactory.createExpression(expFirst, systemConfig);
            IExpression thenExpression = ExpressionCreatorFactory.createExpression(expSecond, systemConfig);
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
            return new CaseSearchedExpression(whenThenExprs, optionalElseExpr);
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to create case searched expression.");
            throw ExecutorException.wrapException(e);
        }
    }
}
