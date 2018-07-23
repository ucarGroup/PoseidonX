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
import com.huawei.streaming.expression.relation.CompareBigDecimal;
import com.huawei.streaming.expression.relation.CompareBoolean;
import com.huawei.streaming.expression.relation.CompareDouble;
import com.huawei.streaming.expression.relation.CompareFloat;
import com.huawei.streaming.expression.relation.CompareInt;
import com.huawei.streaming.expression.relation.CompareLong;
import com.huawei.streaming.expression.relation.CompareString;
import com.huawei.streaming.expression.relation.ICompare;

/**
 * 关系表达式
 * <功能详细描述>
 *
 */
public class RelationExpression extends OperatorBasedExpression implements IBooleanExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -1634180937070359007L;
    
    private static final Logger LOG = LoggerFactory.getLogger(RelationExpression.class);
    
    /**
     * 具体比较器
     */
    private ICompare concreteComp = null;
    
    /**
     * <默认构造函数>
     *
     */
    public RelationExpression(ExpressionOperator op, IExpression leftExpr, IExpression rightExpr)
        throws IllegalDataTypeException
    {
        super(op, leftExpr, rightExpr);
        setType(Boolean.class);
        
        Class< ? > typeOne = StreamClassUtil.getWrapType(leftExpr.getType());
        Class< ? > typeTwo = StreamClassUtil.getWrapType(rightExpr.getType());
        Class< ? > compareType = StreamClassUtil.getCompareType(typeOne, typeTwo);
        
        if (compareType == String.class)
        {
            concreteComp = new CompareString();
            return;
        }
        
        if (compareType == Boolean.class)
        {
            concreteComp = new CompareBoolean();
            return;
        }
        
        if (!StreamClassUtil.isNumberic(compareType))
        {
            IllegalDataTypeException exception =
                new IllegalDataTypeException(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE, typeOne.getName(),
                    typeTwo.getName());
            LOG.error(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE.getFullMessage(), exception);
            throw exception;
        }
        
        if (compareType == BigDecimal.class)
        {
            concreteComp = new CompareBigDecimal();
        }
        else if (compareType == Double.class)
        {
            concreteComp = new CompareDouble();
        }
        else if (compareType == Float.class)
        {
            concreteComp = new CompareFloat();
        }
        else if (compareType == Long.class)
        {
            concreteComp = new CompareLong();
        }
        else if (compareType == Integer.class)
        {
            concreteComp = new CompareInt();
        }
        else
        {
            IllegalDataTypeException exception =
                new IllegalDataTypeException(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE, typeOne.getName(),
                    typeTwo.getName());
            LOG.error(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE.getFullMessage(typeOne.getName(),
                typeTwo.getName()), exception);
            throw exception;
        }
        
        return;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        if (theEvent == null)
        {
            throw new IllegalArgumentException("event == null");
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
            throw new RuntimeException("Streams events are null.");
        }
        Object lo = this.getLeftExpr().evaluate(eventsPerStream);
        Object ro = this.getRightExpr().evaluate(eventsPerStream);
        
        return compute(lo, ro);
    }
    
    /**
     * 对type进行赋值
     *
     */
    public void setType(Class< ? > type)
    {
        if (type == Boolean.class || type == boolean.class)
        {
            super.setType(Boolean.class);
        }
        else if (type != null)
        {
            String msg = "Logic expression type must be boolean class or Booelan class. wrong type :" + type.toString();
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        else
        {
            String msg = "Type is null";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * <根据表达式和运算，预测结果类型>
     * <功能详细描述>
     *
     */
    public Class< ? > validateType()
    {
        return Boolean.class;
    }
    
    /**
     * 计算表达式结果
     * <功能详细描述>
     *
     */
    private Object compute(Object lo, Object ro)
    {
        //如果关系运算的任何一方对象为NULL，则返回结果为NULL
        if (null == lo || null == ro)
        {
            return null;
        }
        
        switch (this.getOp())
        {
            case EQUAL:
                return concreteComp.equals(lo, ro);
            case NOT_EQUAL:
                return concreteComp.notEquals(lo, ro);
            case GREATERTHAN:
                return concreteComp.greaterThan(lo, ro);
            case LESSTHAN:
                return concreteComp.lessThan(lo, ro);
            case LESSTHAN_EQUAL:
                return concreteComp.lessOrEquals(lo, ro);
            case GREATERTHAN_EQUAL:
                return concreteComp.greaterOrEquals(lo, ro);
            default:
                throw new RuntimeException("unknown relation operator!\n");
        }
        
    }
    
}
