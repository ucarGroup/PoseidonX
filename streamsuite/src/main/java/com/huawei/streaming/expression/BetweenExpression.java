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
 * <Between表达式, 支持string 和 number 类型,且 Between 右表达式为常量表达式，且低值和高值类型一致. >= low and <= high>
 * <例如： a between 1 and 10 ， a not between 1 and 10.>
 * <例如： a between "a" and "ab" ， a not between "a" and "ab".>
 *
 */
public class BetweenExpression implements IBooleanExpression
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -5084594776034138568L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(BetweenExpression.class);
    
    /**
     * 是否为Between， true = Between， false = not Between
     */
    private final boolean isBetween;
    
    /**
     * 左表达式
     */
    private final IExpression leftExpr;
    
    /**
     * 右表达式中低值
     */
    private final ConstExpression lowExpr;
    
    /**
     * 右表达式中高值
     */
    private final ConstExpression highExpr;
    
    /**
     * 是否字符串比较
     */
    private boolean isString;
    
    /**
     * 是否数字比较
     */
    private boolean isNumberic;
    
    /**
     * <默认构造函数>
     *
     */
    public BetweenExpression(IExpression leftExpr, ConstExpression lowExpr, ConstExpression highExpr, boolean isBetween)
        throws StreamingException
    {
        
        if (leftExpr == null || lowExpr == null || highExpr == null)
        {
            LOG.error("Arguments in '{}' operator is null.", this.getClass().getName());
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        Class< ? > leftType = StreamClassUtil.getWrapType(leftExpr.getType());
        
        if (leftType == String.class)
        {
            if (lowExpr.getType() != String.class || highExpr.getType() != String.class)
            {
                StreamingException exception =
                    new StreamingException(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE, lowExpr.getType()
                        .getName(), highExpr.getType().getName());
                LOG.error(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE.getFullMessage(lowExpr.getType().getName(),
                    highExpr.getType().getName()), exception);
                throw exception;
            }
            
            this.isString = true;
        }
        
        if (StreamClassUtil.isNumberic(leftType))
        {
            if (!StreamClassUtil.isNumberic(lowExpr.getType()) || !StreamClassUtil.isNumberic(highExpr.getType()))
            {
                StreamingException exception =
                    new StreamingException(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE, lowExpr.getType()
                        .getName(), highExpr.getType().getName());
                LOG.error(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE.getFullMessage(lowExpr.getType()
                    .getName(), highExpr.getType().getName()), exception);
                throw exception;
            }
            
            this.isNumberic = true;
        }
        
        this.leftExpr = leftExpr;
        this.lowExpr = lowExpr;
        this.highExpr = highExpr;
        this.isBetween = isBetween;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        Object value = leftExpr.evaluate(theEvent);
        Object low = lowExpr.evaluate(theEvent);
        Object high = highExpr.evaluate(theEvent);
        
        return compute(value, low, high);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        Object value = leftExpr.evaluate(eventsPerStream);
        Object low = lowExpr.evaluate(eventsPerStream);
        Object high = highExpr.evaluate(eventsPerStream);
        
        return compute(value, low, high);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Class< ? > getType()
    {
        return Boolean.class;
    }
    
    public IExpression getLeftExpr()
    {
        return leftExpr;
    }
    
    public ConstExpression getLowExpr()
    {
        return lowExpr;
    }
    
    public ConstExpression getHighExpr()
    {
        return highExpr;
    }
    
    /**
     * <计算表达式值>
     *
     */
    private Object compute(Object value, Object low, Object high)
    {
        if (value == null)
        {
            return null;
        }
        
        boolean result = true;
        
        if (isString)
        {
            String valueStr = (String)value;
            String lowStr = (String)low;
            String highStr = (String)high;
            
            if (highStr.compareTo(lowStr) < 0)
            {
                String temp = highStr;
                highStr = lowStr;
                lowStr = temp;
            }
            
            if (valueStr.compareTo(lowStr) < 0)
            {
                result = false;
            }
            if (valueStr.compareTo(highStr) > 0)
            {
                result = false;
            }
            
        }
        
        if (isNumberic)
        {
            Double valueNum = ((Number)value).doubleValue();
            Double lowNum = ((Number)low).doubleValue();
            Double highNum = ((Number)high).doubleValue();
            
            if (highNum < lowNum)
            {
                Double temp = highNum;
                highNum = lowNum;
                lowNum = temp;
            }
            
            if (valueNum < lowNum)
            {
                result = false;
            }
            if (valueNum > highNum)
            {
                result = false;
            }
        }
        
        if (isBetween)
        {
            return result;
        }
        else
        {
            return !result;
        }
    }
    
}
