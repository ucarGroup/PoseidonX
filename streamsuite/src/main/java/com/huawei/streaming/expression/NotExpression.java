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

package com.huawei.streaming.expression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.StreamClassUtil;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * <Not表达式>
 * <NOT(Expression)>
 * 
 */
public class NotExpression implements IBooleanExpression
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 2626359887062199473L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(NotExpression.class);
    
    /**
     * Inner_expression
     */
    private IExpression innerExpr;
    
    /**
     * <默认构造函数>
     *@param innerExpr 子表达式
     *@throws StreamingException 表达式构建异常
     */
    public NotExpression(IExpression innerExpr)
        throws StreamingException
    {
        if (innerExpr == null)
        {
            LOG.error("Arguments in '{}' operator is null.", this.getClass().getName());
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        Class< ? > childType = innerExpr.getType();
        if (!StreamClassUtil.isBoolean(childType))
        {
            StreamingException exception =
                new StreamingException(ErrorCode.SEMANTICANALYZE_NOT_EXPRESSION_BOOLEAN_TYPE);
            LOG.error(ErrorCode.SEMANTICANALYZE_NOT_EXPRESSION_BOOLEAN_TYPE.getFullMessage(), exception);
            throw exception;
        }
        
        this.innerExpr = innerExpr;
    }
    
    /** {@inheritDoc} */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        Boolean value = (Boolean)innerExpr.evaluate(theEvent);
        if (value == null)
        {
            return null;
        }
        
        return !value;
    }
    
    /** {@inheritDoc} */
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        Boolean value = (Boolean)innerExpr.evaluate(eventsPerStream);
        if (value == null)
        {
            return null;
        }
        
        return !value;
    }
    
    /** {@inheritDoc} */
    
    @Override
    public Class< ? > getType()
    {
        return Boolean.class;
    }
    
    public IExpression getInnerExpr()
    {
        return innerExpr;
    }
    
}
