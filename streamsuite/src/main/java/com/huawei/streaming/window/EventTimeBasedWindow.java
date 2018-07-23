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

package com.huawei.streaming.window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.StreamClassUtil;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IRenew;
import com.huawei.streaming.view.ViewImpl;

/**
 * <基于事件的时间戳驱动窗口抽象类>
 * <功能详细描述>
 * 
 */
public abstract class EventTimeBasedWindow extends ViewImpl implements IWindow, IRenew
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 7201848146148175755L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(EventTimeBasedWindow.class);
    
    /**
     * 时间属性表达式
     */
    private IExpression timeExpr;
    
    /**
     * 窗口事件保存时间
     */
    private final long keepTime;
    
    /**
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;

    /**
     * 窗口表达式是否数值类型
     */
    private boolean isNumberic = false;

    /**
     * 窗口表达式是否时间类型
     */
    private boolean isDate = false;
    
    /**
     * <默认构造函数>
     *@param keepTime 窗口事件保存时间
     *@param timeExpr 时间属性表达式
     */
    public EventTimeBasedWindow(long keepTime, IExpression timeExpr)
    {
        if (keepTime > 0)
        {
            this.keepTime = keepTime;
            LOG.debug("Time window KeepTime: {}.", keepTime);
        }
        else
        {
            LOG.error("Invalid keepTime: {}.", keepTime);
            throw new IllegalArgumentException("Invalid keepTime: " + keepTime);
        }
        
        if (null == timeExpr)
        {
            String msg = "Time expression is null.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        validate(timeExpr);
    }
    
    /**
     * <返回事件时间信息>
     */
    protected Long getTimestamp(IEvent event)
    {
        if (isNumberic)
        {
            Number num = (Number)timeExpr.evaluate(event);
            return num.longValue(); 
        }
        else
        {
            //所有的时间类型均继承自java.util.Date
            java.util.Date date = (java.util.Date)timeExpr.evaluate(event);
            return date.getTime();
        }
    }
    
    /**
     * 返回时间属性表达式
     */
    protected final IExpression getTimeExpr()
    {
        return timeExpr;
    }
    
    /**
     * 返回窗口事件保存时间
     */
    protected final long getKeepTime()
    {
        return keepTime;
    }
    
    /**
     * {@inheritDoc}
     */
    public void setDataCollection(IDataCollection dataCollection)
    {
        if (dataCollection == null)
        {
            LOG.error("Invalid dataCollection.");
            throw new IllegalArgumentException("Invalid dataCollection");
        }
        
        this.dataCollection = dataCollection;
    }
    
    /**
     * 返回窗口事件缓存集
     */
    public IDataCollection getDataCollection()
    {
        return dataCollection;
    }
    
    private void validate(IExpression timeExpr)  
    {
        Class< ? > timeType = StreamClassUtil.getWrapType(timeExpr.getType());
        
        if (StreamClassUtil.isDateOrTimestamp(timeType))
        {      
            this.isDate  = true;
        }
        else if (StreamClassUtil.isNumberic(timeType))
        {   
            this.isNumberic = true;
        }
        else
        {
            String msg = "Time expression is not Nubmeric or Date or Timestamp Type.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }

        this.timeExpr = timeExpr;
    }
}
