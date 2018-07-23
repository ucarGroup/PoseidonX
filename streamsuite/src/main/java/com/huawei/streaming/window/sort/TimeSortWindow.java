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

package com.huawei.streaming.window.sort;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.CollectionUtil;
import com.huawei.streaming.common.StreamClassUtil;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.timeservice.TimeService;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.window.TimeBasedWindow;

/**
 * <基于时间的比较窗口>
 * <功能详细描述>
 * 
 */
public class TimeSortWindow extends TimeBasedWindow
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -8523027802570632549L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimeSortWindow.class);
    
    /**
     * 时间属性表达式
     */
    private IExpression expr;
    
    /**
     * 窗口排序事件
     */
    private TreeMap<Object, Object> sortedEvents;
    
    /**
     * 事件数量
     */
    private int eventCount;
    
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
     *@param keepTime 窗口保持时间
     *@param expr 时间属性表达式
     */
    public TimeSortWindow(long keepTime, IExpression expr)
    {
        super(keepTime);
        
        if (null == expr)
        {
            String msg = "Time expression is null.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        validate(expr);
        this.sortedEvents = new TreeMap<Object, Object>();
        TimeService timeservice = new TimeService(keepTime, this);
        setTimeservice(timeservice);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        try
        {
            lock();
            LOG.debug("Current window event count: {}.", eventCount);
            internalUpdate(newData, oldData);
        }
        finally
        {
            if (isLocked())
            {
                unlock();
            }
        }
    }
    
    /**
     * <事件内部处理， 上一个窗口旧事件在当前窗口存在则输出，上一个窗口新事件如果小于当前窗口时间范围则输出，否则加入当前窗口>
     */
    private void internalUpdate(IEvent[] newData, IEvent[] oldData)
    {
        ArrayList<IEvent> removedEvents = null;
        if (oldData != null)
        {
            for (int i = 0; i < oldData.length; i++)
            {
                IEvent event = oldData[i];
                Object sortValues = getTimestamp(event);
                
                boolean result = CollectionUtil.removeEventByKey(sortValues, event, sortedEvents);
                if (result)
                {
                    eventCount--;
                    if (removedEvents == null)
                    {
                        removedEvents = new ArrayList<IEvent>();
                    }
                    removedEvents.add(event);
                }
            }
        }
        
        if ((newData != null) && (newData.length > 0))
        {
            long windowTailTime = System.currentTimeMillis() - getKeepTime() + 1;
            long oldestEvent = Long.MAX_VALUE;
            if (!sortedEvents.isEmpty())
            {
                oldestEvent = (Long)sortedEvents.firstKey();
            }
            
            for (int i = 0; i < newData.length; i++)
            {
                IEvent event = newData[i];
                Long timestamp = getTimestamp(event);
                
                if (timestamp < windowTailTime)
                {
                    if (removedEvents == null)
                    {
                        removedEvents = new ArrayList<IEvent>();
                    }
                    removedEvents.add(event);
                }
                
                else
                {
                    if (timestamp < oldestEvent)
                    {
                        oldestEvent = timestamp;
                    }
                    
                    // add to list
                    CollectionUtil.addEventByKeyIntoBack(timestamp, event, sortedEvents);
                    eventCount++;
                }
            }
            
        }
        
        // update child views
        if (this.hasViews())
        {
            IEvent[] expireData = null;
            if (removedEvents != null)
            {
                expireData = removedEvents.toArray(new IEvent[removedEvents.size()]);
            }
            
            IDataCollection dataCollection = getDataCollection();
            if (dataCollection != null)
            {
                dataCollection.update(newData, expireData);
            }
            
            updateChild(newData, expireData);
        }
    }
    
    /**
     * <返回事件时间信息>
     */
    private Long getTimestamp(IEvent event)
    {
        if (isNumberic)
        {
            Number num = (Number)expr.evaluate(event);
            return num.longValue(); 
        }
        else
        {
            java.util.Date date = (java.util.Date)expr.evaluate(event);
            return date.getTime();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void timerCallBack(long currentTime)
    {
        List<IEvent> removedEvents = null;
        try
        {
            lock();
            Long oldestKey;
            
            while (true)
            {
                if (sortedEvents.isEmpty())
                {
                    break;
                }
                
                oldestKey = (Long)sortedEvents.firstKey();
                if (oldestKey >= (currentTime - getKeepTime()))
                {
                    break;
                }
                Object oldestEvents = sortedEvents.remove(oldestKey);
                
                if (oldestEvents != null)
                {
                    if (oldestEvents instanceof List)
                    {
                        @SuppressWarnings("unchecked")
                        List<IEvent> theEvents = (List<IEvent>)oldestEvents;
                        if (removedEvents == null)
                        {
                            removedEvents = theEvents;
                        }
                        else
                        {
                            removedEvents.addAll(theEvents);
                        }
                        eventCount -= theEvents.size();
                    }
                    else
                    {
                        IEvent theEvent = (IEvent)oldestEvents;
                        if (removedEvents == null)
                        {
                            removedEvents = new ArrayList<IEvent>();
                        }
                        removedEvents.add(theEvent);
                        eventCount--;
                    }
                }
                
            }
            
            if (this.hasViews())
            {
                if ((removedEvents != null) && (!removedEvents.isEmpty()))
                {
                    IEvent[] expireData = removedEvents.toArray(new IEvent[removedEvents.size()]);
                    updateChild(null, expireData);
                }
            }
        }
        finally
        {
            if (isLocked())
            {
                unlock();
            }
        }
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

        this.expr = timeExpr;
    }
    
}
