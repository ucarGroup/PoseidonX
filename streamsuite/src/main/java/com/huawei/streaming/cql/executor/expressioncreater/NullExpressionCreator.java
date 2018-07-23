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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.NullExpressionDesc;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.ExprIsNullExpression;
import com.huawei.streaming.expression.IExpression;

/**
 * 表达式是否为空的判断实例
 * 
 */
public class NullExpressionCreator implements ExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(NullExpressionCreator.class);
    
    private NullExpressionDesc expressiondesc;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IExpression createInstance(ExpressionDescribe expressionDescribe, Map<String, String> systemConfig)
        throws ExecutorException
    {
        LOG.info("start to create null expression");
        expressiondesc = (NullExpressionDesc)expressionDescribe;
        try
        {
            return new ExprIsNullExpression(ExpressionCreatorFactory.createExpression(expressiondesc.getExpression(),
                systemConfig), expressiondesc.isNull());
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
}
