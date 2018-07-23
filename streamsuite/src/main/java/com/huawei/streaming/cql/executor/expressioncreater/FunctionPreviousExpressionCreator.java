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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionPreviousDesc;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.PreviousExpression;

/**
 * 创建previous表达式
 * 
 */
public class FunctionPreviousExpressionCreator implements ExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionPreviousExpressionCreator.class);
    
    private FunctionPreviousDesc expressiondesc;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IExpression createInstance(ExpressionDescribe expressionDescribe, Map<String, String> systemConfig)
        throws ExecutorException
    {
        LOG.info("start to create function previous expression");
        expressiondesc = (FunctionPreviousDesc)expressionDescribe;
        IExpression indexExpr =
            ExpressionCreatorFactory.createExpression(expressiondesc.getPreviouNumber(), systemConfig);
        IExpression proExpr =
            ExpressionCreatorFactory.createExpression(expressiondesc.getPreviouCols().get(0), systemConfig);
        try
        {
            return new PreviousExpression(indexExpr, proExpr);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
        
    }
}
