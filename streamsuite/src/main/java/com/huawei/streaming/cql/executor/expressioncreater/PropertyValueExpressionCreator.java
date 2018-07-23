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

import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.PropertyValueExpression;

/**
 * propertyValue表达式创建
 *
 */
public class PropertyValueExpressionCreator implements ExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(PropertyValueExpressionCreator.class);
    
    private PropertyValueExpressionDesc expressiondesc;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IExpression createInstance(ExpressionDescribe expressionDescribe, Map<String, String> systemconfig)
        throws ExecutorException
    {
        expressiondesc = (PropertyValueExpressionDesc)expressionDescribe;
        Class< ? > type = getExpressionTypeClass();
        
        try
        {
            return new PropertyValueExpression(expressiondesc.getIndexInSchemas(), expressiondesc.getProperty(), type);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    private Class< ? > getExpressionTypeClass()
        throws ExecutorException
    {
        try
        {
            return Class.forName(expressiondesc.getType().getName(), true, CQLUtils.getClassLoader());
        }
        catch (ClassNotFoundException e)
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_UNSUPPORTED_DATATYPE, expressiondesc.getType()
                    .getName());
            LOG.error("Unsupport expression type.", exception);
            
            throw exception;
        }
    }
}
