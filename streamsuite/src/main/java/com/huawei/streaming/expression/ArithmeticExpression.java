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

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.StreamClassUtil;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.IllegalDataTypeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;
import com.huawei.streaming.expression.arithmetic.ComputeBigDecimal;
import com.huawei.streaming.expression.arithmetic.ComputeDouble;
import com.huawei.streaming.expression.arithmetic.ComputeFloat;
import com.huawei.streaming.expression.arithmetic.ComputeInt;
import com.huawei.streaming.expression.arithmetic.ComputeLong;
import com.huawei.streaming.expression.arithmetic.ICompute;

/**
 * 
 * <算术表达式>
 * 
 */
public class ArithmeticExpression extends OperatorBasedExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -8302819260272069504L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(ArithmeticExpression.class);
    
    private ICompute computer = null;
    
    /**
     * <默认构造函数>
     */
    public ArithmeticExpression(ExpressionOperator op, IExpression leftExpr, IExpression rightExpr)
        throws StreamingException
    {
        super(op, leftExpr, rightExpr);
        
        Class< ? > leftType = StreamClassUtil.getWrapType(leftExpr.getType());
        Class< ? > rightType = StreamClassUtil.getWrapType(rightExpr.getType());
        
        if (!StreamClassUtil.isNumberic(leftType))
        {
            StreamingException exception =
                new StreamingException(ErrorCode.SEMANTICANALYZE_ARITHMETIC_EXPRESSION_NUMBER_TYPE);
            LOG.error(ErrorCode.SEMANTICANALYZE_ARITHMETIC_EXPRESSION_NUMBER_TYPE.getFullMessage(), exception);
            throw exception;
        }
        
        if (!StreamClassUtil.isNumberic(rightType))
        {
            StreamingException exception =
                new StreamingException(ErrorCode.SEMANTICANALYZE_ARITHMETIC_EXPRESSION_NUMBER_TYPE);
            LOG.error(ErrorCode.SEMANTICANALYZE_ARITHMETIC_EXPRESSION_NUMBER_TYPE.getFullMessage(), exception);
            throw exception;
        }
        
        setType(validateType());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        if (null == theEvent)
        {
            throw new StreamingRuntimeException("IEvent is null!");
        }
        
        IExpression leftExpr = getLeftExpr();
        IExpression rightExpr = getRightExpr();
        
        Object lo = leftExpr.evaluate(theEvent);
        Object ro = rightExpr.evaluate(theEvent);
        
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
        
        IExpression leftExpr = getLeftExpr();
        IExpression rightExpr = getRightExpr();
        
        Object lo = leftExpr.evaluate(eventsPerStream);
        Object ro = rightExpr.evaluate(eventsPerStream);
        
        return compute(lo, ro);
    }
    
    /**
     * <算术运算>
     */
    private Object compute(Object left, Object right)
    {
        //如果算术运算的任何一方对象为NULL，则返回结果为NULL
        if (null == left || null == right)
        {
            return null;
        }
        
        switch (this.getOp())
        {
            case ADD:
                return computer.add((Number)left, (Number)right);
            case SUBTRACT:
                return computer.subtract((Number)left, (Number)right);
            case MULTIPLY:
                return computer.multiply((Number)left, (Number)right);
            case DIVIDE:
                return computer.divide((Number)left, (Number)right);
            case MOD:
                return computer.mod((Number)left, (Number)right);
            default:
                throw new StreamingRuntimeException("unknown relation operator!\n");
        }
    }
    
    /**
     * 对type进行赋值
     */
    public void setType(Class< ? > type)
    {
        if (!StreamClassUtil.isNumberic(type))
        {
            String msg = "";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        Class< ? > warpType = StreamClassUtil.getWrapType(type);
        
        super.setType(warpType);
        
        if (warpType == BigDecimal.class)
        {
            computer = new ComputeBigDecimal();
        }
        else if (warpType == Double.class)
        {
            computer = new ComputeDouble();
        }
        else if (warpType == Float.class)
        {
            computer = new ComputeFloat();
        }
        else if (warpType == Long.class)
        {
            computer = new ComputeLong();
        }
        else if (warpType == Integer.class)
        {
            computer = new ComputeInt();
        }
        else
        {
            throw new IllegalArgumentException("leftExpr or rightExpr is not comtuperable.");
        }
        
        return;
    }
    
    /**
     * <根据表达式和运算，预测结果类型>
     * <功能详细描述>
     */
    public Class< ? > validateType()
    {
        Class< ? > resultType = null;
        Class< ? > leftType = getLeftExpr().getType();
        Class< ? > rightType = getRightExpr().getType();
        
        try
        {
            if (leftType.equals(rightType))
            {
                resultType = StreamClassUtil.getWrapType(rightType);
            }
            else
            {
                resultType = StreamClassUtil.getArithmaticType(leftType, rightType);
            }
        }
        catch (IllegalDataTypeException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
        
        return resultType;
    }
}
