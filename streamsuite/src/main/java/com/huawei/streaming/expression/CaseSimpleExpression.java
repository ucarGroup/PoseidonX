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

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.common.StreamClassUtil;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.IllegalDataTypeException;
import com.huawei.streaming.exception.StreamingException;

/**
 * <Simple CASE expression,通过将表达式与一组简单的表达式进行比较来确定结果。
 * input_expression 及每个 when_expression 的数据类型必须相同或必须是隐式转换的数据类型。
 * 从 result_expressions 和可选 else_result_expression 的类型集中返回优先级最高的类型。>
 * <p/>
 * <先计算input_expression的值，然后与所有在when子句中的声明的when_expression进行比较，直到找到一个相等的返回对应的then子句中result_expression。
 * 如果没有找到匹配的，则返回else子句中else_result_expression（或者Null）。语法如下：
 * CASE input_expression
 * WHEN when_expression THEN result_expression
 * [ ...n ]
 * [ ELSE else_result_expression ]
 * END >
 *
 */
public class CaseSimpleExpression implements IExpression
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 2523247855706327690L;
    
    private static final Logger LOG = LoggerFactory.getLogger(CaseSimpleExpression.class);
    
    private Class< ? > resultType;
    
    private Class< ? > compareType;
    
    private boolean isNumbericResult = false;
    
    private boolean isCompareInfer = false;
    
    private IExpression inputExpr = null;
    
    private List<Pair<IExpression, IExpression>> whenThenExprs = null;
    
    private IExpression optionalElseExpr = null;
    
    /**
     * <默认构造函数>
     *
     */
    public CaseSimpleExpression(IExpression inputExpr, List<Pair<IExpression, IExpression>> whenThenExprs,
        IExpression optionalElseExpr)
        throws StreamingException
    {
        if (inputExpr == null)
        {
            StreamingException exception = new StreamingException(ErrorCode.SEMANTICANALYZE_CASE_WHEN_WHEN_MUST);
            LOG.error(ErrorCode.SEMANTICANALYZE_CASE_WHEN_WHEN_MUST.getFullMessage(), exception);
            throw exception;
        }
        
        List<Class< ? >> compareTypes = new LinkedList<Class< ? >>();
        compareTypes.add(inputExpr.getType());
        if (whenThenExprs == null || whenThenExprs.size() < 1)
        {
            StreamingException exception = new StreamingException(ErrorCode.SEMANTICANALYZE_CASE_WHEN_MORE_WHEN_THEN);
            LOG.error(ErrorCode.SEMANTICANALYZE_CASE_WHEN_MORE_WHEN_THEN.getFullMessage(), exception);
            throw exception;
        }
        
        List<Class< ? >> childTypes = new LinkedList<Class< ? >>();
        for (Pair<IExpression, IExpression> whenThenExpr : whenThenExprs)
        {
            if (whenThenExpr.getFirst() == null || whenThenExpr.getSecond() == null)
            {
                StreamingException exception =
                    new StreamingException(ErrorCode.SEMANTICANALYZE_CASE_WHEN_MUST_WHEN_THEN);
                LOG.error(ErrorCode.SEMANTICANALYZE_CASE_WHEN_MUST_WHEN_THEN.getFullMessage(), exception);
                throw exception;
            }
            compareTypes.add(whenThenExpr.getFirst().getType());
            childTypes.add(whenThenExpr.getSecond().getType());
        }
        
        if (optionalElseExpr != null)
        {
            childTypes.add(optionalElseExpr.getType());
        }
        
        validateCompareType(compareTypes);
        validateResultType(childTypes);
        
        this.inputExpr = inputExpr;
        this.whenThenExprs = whenThenExprs;
        this.optionalElseExpr = optionalElseExpr;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        Object result = null;
        boolean isPass = false;
        
        Object inputValue = inputExpr.evaluate(theEvent);
        
        for (Pair<IExpression, IExpression> whenThenExpr : whenThenExprs)
        {
            Object whenValue = whenThenExpr.getFirst().evaluate(theEvent);
            
            if (compare(inputValue, whenValue))
            {
                result = whenThenExpr.getSecond().evaluate(theEvent);
                isPass = true;
                break;
            }
        }
        
        if (!isPass && optionalElseExpr != null)
        {
            result = optionalElseExpr.evaluate(theEvent);
        }
        
        return processResult(result);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        Object result = null;
        boolean isPass = false;
        
        Object inputValue = inputExpr.evaluate(eventsPerStream);
        
        for (Pair<IExpression, IExpression> whenThenExpr : whenThenExprs)
        {
            Object whenValue = whenThenExpr.getFirst().evaluate(eventsPerStream);
            
            if (compare(inputValue, whenValue))
            {
                result = whenThenExpr.getSecond().evaluate(eventsPerStream);
                isPass = true;
                break;
            }
        }
        
        if (!isPass && optionalElseExpr != null)
        {
            result = optionalElseExpr.evaluate(eventsPerStream);
        }
        
        return processResult(result);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Class< ? > getType()
    {
        return resultType;
    }
    
    public IExpression getInputExpr()
    {
        return inputExpr;
    }
    
    public List<Pair<IExpression, IExpression>> getWhenThenExprs()
    {
        return whenThenExprs;
    }
    
    public IExpression getOptionalElseExpr()
    {
        return optionalElseExpr;
    }
    
    /**
     * <根据inputExpr和whenExpr校验类型，inputExpr和WhenExpr类型必须可比较>
     * <功能详细描述>
     *
     */
    private void validateCompareType(List<Class< ? >> compareTypes)
        throws IllegalDataTypeException
    {
        compareType = StreamClassUtil.getCommonType(compareTypes);
        if (StreamClassUtil.isNumberic(compareType))
        {
            for (Class< ? > type : compareTypes)
            {
                if (type != compareType)
                {
                    isCompareInfer = true;
                }
            }
        }
    }
    
    /**
     * <根据thenExpr和elseExpr校验类型，从 result_expressions和可选 else_result_expression 的类型返回优先级最高的类型。>
     * <功能详细描述>
     *
     */
    private void validateResultType(List<Class< ? >> childTypes)
        throws IllegalDataTypeException
    {
        if (!childTypes.isEmpty())
        {
            resultType = StreamClassUtil.getCommonType(childTypes);
            if (StreamClassUtil.isNumberic(resultType))
            {
                isNumbericResult = true;
            }
        }
    }
    
    private boolean compare(Object inputValue, Object whenValue)
    {
        if (inputValue == null)
        {
            return whenValue == null;
        }
        
        if (whenValue == null)
        {
            return false;
        }
        
        if (isCompareInfer)
        {
            inputValue = StreamClassUtil.getNumbericValueForType((Number)inputValue, compareType);
            whenValue = StreamClassUtil.getNumbericValueForType((Number)whenValue, compareType);
        }
        
        return inputValue.equals(whenValue);
    }
    
    private Object processResult(Object result)
    {
        if (result == null)
        {
            return null;
        }
        
        if (isNumbericResult && result.getClass() != resultType)
        {
            result = StreamClassUtil.getNumbericValueForType((Number)result, resultType);
        }
        return result;
    }
    
}
