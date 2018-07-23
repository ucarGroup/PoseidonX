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

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * <判断表达式为否为NUll表达式>
 * <功能详细描述>
 *
 */
public class ExprIsNullExpression implements IBooleanExpression
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 4708456433153506780L;
    
    /**
     * 日志对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(ExprIsNullExpression.class);
    
    /**
     * 待判断表达式
     */
    private IExpression expr;
    
    /**
     * 是否Null判断，true = is Null ， false = is not Null
     */
    private boolean isNull;
    
    /**
     * <默认构造函数>
     *
     */
    public ExprIsNullExpression(IExpression expr, Boolean isNull)
        throws StreamingException
    {
        if (expr == null || isNull == null)
        {
            LOG.error("Arguments in '{}' operator is null.", this.getClass().getName());
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        this.expr = expr;
        this.isNull = isNull;
    }
    
    /**
     * {@inheritDoc}
     */
    
    @Override
    public Object evaluate(IEvent theEvent)
    {
        if (theEvent == null)
        {
            LOG.error("Stream event is null.");
            throw new IllegalArgumentException("Stream event is null.");
        }
        
        Object lo = this.expr.evaluate(theEvent);
        
        return (lo == null) == isNull;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        if (null == eventsPerStream || 0 == eventsPerStream.length)
        {
            LOG.error("Streams events are null.");
            throw new RuntimeException("Streams events are null.");
        }
        
        Object lo = this.expr.evaluate(eventsPerStream);
        
        return (lo == null) == isNull;
    }
    
    /**
     * {@inheritDoc}
     */
    
    @Override
    public Class< ? > getType()
    {
        return Boolean.class;
    }
    
    public IExpression getExpr()
    {
        return expr;
    }
    
}
