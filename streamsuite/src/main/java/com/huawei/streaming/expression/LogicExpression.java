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
import com.huawei.streaming.exception.StreamingRuntimeException;

/**
 * 
 * 逻辑表达式
 * <功能详细描述>
 * 
 */
public class LogicExpression extends OperatorBasedExpression implements IBooleanExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -5866922189009594336L;
    
    private static final Logger LOG = LoggerFactory.getLogger(LogicExpression.class);
    
    /**
     * <默认构造函数>
     *@param op 运算操作
     *@param leftExpr 左表达式
     *@param rightExpr 右表达式
     *@throws StreamingException 表达式构建异常
     */
    public LogicExpression(ExpressionOperator op, IExpression leftExpr, IExpression rightExpr)
        throws StreamingException
    {
        super(op, leftExpr, rightExpr);
        
        Class< ? > leftType = StreamClassUtil.getWrapType(leftExpr.getType());
        Class< ? > rightType = StreamClassUtil.getWrapType(rightExpr.getType());
        
        if ((leftType != Boolean.class) || (rightType != Boolean.class))
        {
            StreamingException exception =
                new StreamingException(ErrorCode.SEMANTICANALYZE_LOGIC_EXPRESSION_BOOLEAN_TYPE);
            LOG.error(ErrorCode.SEMANTICANALYZE_LOGIC_EXPRESSION_BOOLEAN_TYPE.getFullMessage(), exception);
            throw exception;
        }
        
        setType(Boolean.class);
    }
    
    /** {@inheritDoc} */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        if (theEvent == null)
        {
            throw new StreamingRuntimeException("Input event is null.");
        }
        
        Object lo = this.getLeftExpr().evaluate(theEvent);
        Object ro = this.getRightExpr().evaluate(theEvent);
        
        return compute(lo, ro);
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
            throw new StreamingRuntimeException("Streams events are null.");
        }
        
        Object lo = this.getLeftExpr().evaluate(eventsPerStream);
        Object ro = this.getRightExpr().evaluate(eventsPerStream);
        
        return compute(lo, ro);
    }
    
  
    private Object compute(Object left, Object right)
    {
        /*
         * null or null = null
         * null and null = null
         * 
         * null and true = null
         * null and false = false
         * null or true = true
         * null or false = null
         */
        
        Object result = checkArguments(left, right);
        if (result == null)
        {
            return null;
        }
        
        /*
         * null or null = null
         * null nul and = null
         */
        if (left == null && right == null)
        {
            return null;
        }
        
        /*
         *都不为null时，正常逻辑运算
         */
        
        if (left != null && right != null)
        {
            return computeNotNull(left, right);
        }
        
        /* 
         * null and true = null
         * null and false = false
         * null or true = true
         * null or false = null
         */
        if (left == null)
        {
            return computeSlideNull(right);
        }
        
        /* 
         * true and null = null
         * false and null  = false
         * true or null = true
         * false or null = null
         */
        
        return computeSlideNull(left);
        
    }
    
    private Object computeSlideNull(Object arg)
    {
        switch (this.getOp())
        {
            case LOGICAND:
            {
                return ((Boolean)arg) ? null : false;
            }
            case LOGICOR:
            {
                return ((Boolean)arg) ? true : null;
            }
            default:
                throw new StreamingRuntimeException("unknown logic operator!");
        }
    }
    
    private Object computeNotNull(Object left, Object right)
    {
        switch (this.getOp())
        {
            case LOGICAND:
            {
                return (Boolean)left && (Boolean)right;
            }
            case LOGICOR:
            {
                return (Boolean)left || (Boolean)right;
            }
            default:
                throw new StreamingRuntimeException("unknown logic operator!");
        }
    }
    
    private Object checkArguments(Object left, Object right)
    {
        
        if (left != null && !((left instanceof Boolean)))
        {
            LOG.warn("The value of leftExpression is invalid, return null. leftvalue={}, rightvalue={}", left, right);
            return null;
        }
        
        if (right != null && !((right instanceof Boolean)))
        {
            LOG.warn("The value of rightExpression is invalid, return null. leftvalue={}, rightvalue={}", left, right);
            return null;
        }
        return true;
    }
    
}
