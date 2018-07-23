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
 * <In表达式，支持string 和 number 类型，且 in 右表达式为常量表达式，且数组中类型一致。>
 * <例如 a in （1， 2）， a not in （ 1, 2)>
 * <例如 a in （"a"， "ab"）， a not in （"a", "ab")>
 *
 */
public class InExpression implements IBooleanExpression
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -5984988244403180302L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(InExpression.class);
    
    /**
     * 是否为In， true = in， false = not in
     */
    private final boolean isIn;
    
    /**
     * 左表达式
     */
    private final IExpression leftExpr;
    
    /**
     * 右表达式数组
     */
    private final ConstExpression[] rightExprs;
    
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
    public InExpression(IExpression leftExpr, ConstExpression[] rightExprs, boolean isIn)
        throws StreamingException
    {
        if (leftExpr == null || rightExprs == null || rightExprs.length < 1)
        {
            LOG.error("Arguments in '{}' operator is null.", this.getClass().getName());
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        Class< ? > leftType = StreamClassUtil.getWrapType(leftExpr.getType());
        
        if (leftType == String.class)
        {
            for (int i = 0; i < rightExprs.length; i++)
            {
                if (rightExprs[i].getType() != String.class)
                {
                    StreamingException exception =
                        new StreamingException(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE,
                            rightExprs[i].getType().getName(), String.class.getName());
                    LOG.error(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE.getFullMessage(
                        rightExprs[i].getType().getName(), String.class.getName()), exception);
                    throw exception;
                }
            }
            
            this.isString = true;
        }
        
        if (StreamClassUtil.isNumberic(leftType))
        {
            for (int i = 0; i < rightExprs.length; i++)
            {
                if (!StreamClassUtil.isNumberic(rightExprs[i].getType()))
                {
                    StreamingException exception =
                        new StreamingException(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE,
                            rightExprs[i].getType().getName(), Number.class.getName());
                    LOG.error(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE.getFullMessage(
                        rightExprs[i].getType().getName(), Number.class.getName()), exception);
                    throw exception;
                }
            }
            
            this.isNumberic = true;
        }
        
        this.leftExpr = leftExpr;
        this.rightExprs = rightExprs;
        this.isIn = isIn;
    }
    
    /**
     * {@inheritDoc}
     */
    
    @Override
    public Object evaluate(IEvent theEvent)
    {
        if (theEvent == null)
        {
            return null;
        }
        
        Object value = leftExpr.evaluate(theEvent);
        
        return compute(value, theEvent);
    }
    
    /**
     * {@inheritDoc}
     */
    
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        
        Object value = leftExpr.evaluate(eventsPerStream);
        
        return compute(value, eventsPerStream[0]);
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
    
    public ConstExpression[] getRightExprs()
    {
        return rightExprs;
    }
    
    /**
     * <计算表达式值>
     *
     */
    private Object compute(Object value, IEvent theEvent)
    {
        if (value == null)
        {
            return null;
        }
        
        if (isString)
        {
            for (int i = 0; i < rightExprs.length; i++)
            {
                Object rightStr = rightExprs[i].evaluate(theEvent);
                if (value.equals(rightStr))
                {
                    return isIn;
                }
            }
        }
        
        if (isNumberic)
        {
            Double valueNum = ((Number)value).doubleValue();
            for (int i = 0; i < rightExprs.length; i++)
            {
                Double rightNum = ((Number)rightExprs[i].evaluate(theEvent)).doubleValue();
                if (valueNum.equals(rightNum))
                {
                    return isIn;
                }
            }
        }
        
        return !isIn;
    }
    
}
