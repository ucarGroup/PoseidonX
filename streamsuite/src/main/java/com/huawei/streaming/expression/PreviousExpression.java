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
import com.huawei.streaming.window.AbstractAccessService;
import com.huawei.streaming.window.IRandomAccessByIndex;
import com.huawei.streaming.window.IRelativeAccessByEventAndIndex;
import com.huawei.streaming.window.RandomAccessByIndexService;
import com.huawei.streaming.window.RelativeAccessByEventAndIndexService;

/**
 * <previous表达式，获取窗口中第几条数据中的指定属性的值>
 * <如果包含Previous Expression，则必须包含窗口（不支持窗口的叠加，仅能有一个窗口）
 *  语法如下：previous(index， property)。其中index为索引信息，property为事件中指定属性名。>
 * 
 */
public class PreviousExpression implements IExpression
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 8398398053090322356L;
    
    /**
     * 日志对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(PreviousExpression.class);
    
    /**
     * 返回结果类型
     */
    private Class< ? > resultType;
    
    /**
     * 从窗口缓存获取事件服务
     */
    private RandomAccessByIndexService randomAccessService;
    
    /**
     * 从窗口缓存获取事件服务
     */
    private RelativeAccessByEventAndIndexService relativeAccessService;
    
    /**
     * 索引表达式
     */
    private IExpression indexExpr;
    
    /**
     * 属性表达式
     */
    private IExpression proExpr;
    
    private int streamIndex;
    
    /**
     * <默认构造函数>
     *@param indexExpr 索引表达式
     *@param proExpr 属性表达式
     *@throws StreamingException 表达式构建异常
     */
    public PreviousExpression(IExpression indexExpr, IExpression proExpr)
        throws StreamingException
    {
        
        if (indexExpr == null || proExpr == null)
        {
            LOG.error("Arguments in '{}' operator is null.", this.getClass().getName());
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        Class< ? > indexType = indexExpr.getType();
        if (!StreamClassUtil.isNumberic(indexType))
        {
            StreamingException exception = new StreamingException(ErrorCode.FUNCTION_PREVIOUS_PROPERTYVALUE_EXPRSSION);
            LOG.error(ErrorCode.FUNCTION_PREVIOUS_PROPERTYVALUE_EXPRSSION.getFullMessage(), exception);
            throw exception;
        }
        
        this.indexExpr = indexExpr;
        this.proExpr = proExpr;
        
        if (proExpr instanceof PropertyValueExpression)
        {
            this.streamIndex = ((PropertyValueExpression)proExpr).getStreamIndex();
        }
        
        resultType = proExpr.getType();
    }
    
    /**
     * <设置Previous表达式访问窗口数据服务>
     * <设置Previous表达式访问窗口数据服务>
     */
    public void setService(AbstractAccessService service)
    {
        if (service == null)
        {
            String msg = "Service is NULl.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (service instanceof RandomAccessByIndexService)
        {
            this.randomAccessService = (RandomAccessByIndexService)service;
        }
        
        if (service instanceof RelativeAccessByEventAndIndexService)
        {
            this.relativeAccessService = (RelativeAccessByEventAndIndexService)service;
        }
        
    }
    
    /** {@inheritDoc} */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        Object value = indexExpr.evaluate(theEvent);
        if (value == null)
        {
            return null;
        }
        
        int indexValue = checkIndexValue(value);
        
        return getProValue(theEvent, indexValue);
    }
    
    private Object getProValue(IEvent theEvent, int indexValue)
    {
        if (randomAccessService != null)
        {
            IRandomAccessByIndex randomAccess = randomAccessService.getAccessor();
            if(randomAccess == null)
            {
                return null;
            }
            IEvent indexEvent = randomAccess.getEvent(indexValue, true);
            if (indexEvent == null)
            {
                return null;
            }
            
            return proExpr.evaluate(indexEvent);
        }
        
        if (relativeAccessService != null)
        {
            IRelativeAccessByEventAndIndex relativeAccess = relativeAccessService.getAccessor();
            if(relativeAccess == null)
            {
                return null;
            }
            IEvent indexEvent = relativeAccess.getEvent(theEvent, indexValue);
            if (indexEvent == null)
            {
                return null;
            }
            
            return proExpr.evaluate(indexEvent);
        }
        
        return null;
    }
    
    private int checkIndexValue(Object value)
    {
        if (!(value instanceof Number))
        {
            String msg = "Previous Expression requires an integer index parameter or expression.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        Number indexValue = (Number)value;
        if (StreamClassUtil.isFloatingPointNumber(indexValue))
        {
            String msg = "Previous Expression requires an integer index parameter or expression.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        return indexValue.intValue();
    }
    
    /** {@inheritDoc} */
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        Object value = indexExpr.evaluate(eventsPerStream);
        if (value == null)
        {
            return null;
        }
        
        int indexValue = checkIndexValue(value);
        
        return getProValue(eventsPerStream, indexValue);
    }
    
    private Object getProValue(IEvent[] eventsPerStream, int indexValue)
    {
        if (randomAccessService != null)
        {
            IRandomAccessByIndex randomAccess = randomAccessService.getAccessor();
            if(randomAccess == null)
            {
                return null;
            }
            IEvent indexEvent = randomAccess.getEvent(indexValue, true);
            if (indexEvent == null)
            {
                return null;
            }
            
            return proExpr.evaluate(indexEvent);
        }
        
        if (relativeAccessService != null)
        {
            IRelativeAccessByEventAndIndex relativeAccess = relativeAccessService.getAccessor();
            if(relativeAccess == null)
            {
                return null;
            }
            IEvent indexEvent = relativeAccess.getEvent(eventsPerStream[streamIndex], indexValue);
            if (indexEvent == null)
            {
                return null;
            }
            
            IEvent originalEvent = eventsPerStream[streamIndex];
            eventsPerStream[streamIndex] = indexEvent;
            Object evaluteValue = proExpr.evaluate(eventsPerStream);
            eventsPerStream[streamIndex] = originalEvent;
            
            return evaluteValue;
        }
        
        return null;
    }
    
    /** {@inheritDoc} */
    @Override
    public Class< ? > getType()
    {
        return resultType;
    }
    
    public IExpression getProExpr()
    {
        return proExpr;
    }
    
    public IExpression getIndexExpr()
    {
        return indexExpr;
    }
    
}
