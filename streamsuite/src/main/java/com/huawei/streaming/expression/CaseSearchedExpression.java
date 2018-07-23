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
 * <Searched CASE expression,通过计算一组布尔表达式来确定结果。从 result_expressions 和可选 else_result_expression 的类型集中返回优先级最高的类型。>
 * <其中when子句中的condition为条件表达式。从前向后对各个Boolean_expression进行测试，如果Boolean_expression为true则返回对应then子句中的result_expression。
 * 如果所有when子句中Boolean_expression都不为true，则返回else子句中else_result_expression（或者Null）。语法如下：
 * CASE
 * WHEN Boolean_expression THEN result_expression
 * [ ...n ]
 * [ ELSE else_result_expression ]
 * END>
 *
 */
public class CaseSearchedExpression implements IExpression
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -1654436358828965533L;
    
    /**
     * 日志对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(CaseSearchedExpression.class);
    
    private Class< ? > resultType;
    
    private boolean isNumbericResult = false;
    
    private List<Pair<IExpression, IExpression>> whenThenExprs = null;
    
    private IExpression optionalElseExpr = null;
    
    /**
     * <默认构造函数>
     *
     */
    public CaseSearchedExpression(List<Pair<IExpression, IExpression>> whenThenExprs, IExpression optionalElseExpr)
        throws StreamingException
    {
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
            
            if (whenThenExpr.getFirst().getType() != Boolean.class)
            {
                StreamingException exception =
                    new StreamingException(ErrorCode.SEMANTICANALYZE_CASE_WHEN_WHEN_BOOLEAN, whenThenExpr.getFirst()
                        .getType().getName());
                LOG.error(ErrorCode.SEMANTICANALYZE_CASE_WHEN_WHEN_BOOLEAN.getFullMessage(whenThenExpr.getFirst()
                    .getType().getName()), exception);
                throw exception;
            }
            
            childTypes.add(whenThenExpr.getSecond().getType());
        }
        
        if (optionalElseExpr != null)
        {
            childTypes.add(optionalElseExpr.getType());
        }
        
        validateResultType(childTypes);
        
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
        
        for (Pair<IExpression, IExpression> whenThenExpr : whenThenExprs)
        {
            Boolean pass = (Boolean)whenThenExpr.getFirst().evaluate(theEvent);
            
            if (pass != null && pass)
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
        
        for (Pair<IExpression, IExpression> whenThenExpr : whenThenExprs)
        {
            Boolean pass = (Boolean)whenThenExpr.getFirst().evaluate(eventsPerStream);
            
            if (pass != null && pass)
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
    
    public List<Pair<IExpression, IExpression>> getWhenThenExprs()
    {
        return whenThenExprs;
    }
    
    public IExpression getOptionalElseExpr()
    {
        return optionalElseExpr;
    }
    
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
