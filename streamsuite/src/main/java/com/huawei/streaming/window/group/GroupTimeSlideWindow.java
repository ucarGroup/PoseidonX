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

package com.huawei.streaming.window.group;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.timeservice.TimeService;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;
import com.huawei.streaming.window.TimeSlideEventList;

/**
 * <基于时间滑动的分组窗口>
 * <对每个分组值对应的窗口进行处理>
 * 
 */
public class GroupTimeSlideWindow extends GroupTimeBasedWindow
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 4847878507453629670L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(GroupTimeSlideWindow.class);
    
    /**
     * 分组窗口中每个分组对应事件 集合
     */
    private final HashMap<Object, Object> eventsPerKey = new HashMap<Object, Object>();
    
    /**
     * 事件
     */
    private TimeSlideEventList events = null;
    
    /**
     * 时间与分组反向索引，根据时间得到分组中具有过期事件，然后中分组事件集合中查找。
     */
    private final TreeMap<Long, HashSet<Object>> timegroups = new TreeMap<Long, HashSet<Object>>();
    
    /**
     * 具有过期事件的分组信息
     */
    private HashSet<Object> timegroup = null;
    
    /**
     * <默认构造函数>
     *@param exprs 分组表达式
     *@param keepTime 窗口保持时间，单位：ms
     *@param slideInterval 窗口滑动间隔，单位：ms
     */
    public GroupTimeSlideWindow(IExpression[] exprs, long keepTime, long slideInterval)
    {
        super(exprs, keepTime);
        
        TimeService timeservice = new TimeService(slideInterval, this);
        setTimeservice(timeservice);
        
    }
    
    /** {@inheritDoc} */
    
    @Override
    protected void processGroupedEvent(long currentTime)
    {
        Iterator<Long> itr = timegroups.keySet().iterator();
        Set<Object> groupKeys = new HashSet<Object>();
        
        while (itr.hasNext())
        {
            long expirTime = itr.next();
            
            if (expirTime >= currentTime)
            {
                break;
            }
            else
            {
                timegroup = timegroups.get(expirTime);
                for (Object groupKey : timegroup)
                {
                    groupKeys.add(groupKey);
                }
                
                itr.remove();
            }
        }
        
        Object subViews = null;
        IDataCollection subCollection = null;
        for (Object obj : groupKeys)
        {
            subViews = getSubViewsPerKey().get(obj);
            subCollection = getSubCollectionPerKey().get(obj);
            events = (TimeSlideEventList)eventsPerKey.get(obj);
            IEvent[] oldData = events.getOldData(currentTime);
            
            if ((null != subViews) && (null != oldData))
            {
                if (subCollection != null)
                {
                    subCollection.update(null, oldData);
                }
                
                if (subViews instanceof IView)
                {
                    ((IView)subViews).update(null, oldData);
                    
                    LOG.debug("Send Slide Window Events For GroupID: {}.", obj);
                }
            }
        }
    }
    
    /** {@inheritDoc} */
    @Override
    protected void processGroupedEvent(Object subViews, IDataCollection subCollection, Object groupKey, IEvent theEvent)
    {
        events = (TimeSlideEventList)eventsPerKey.get(groupKey);
        if (null == events)
        {
            events = new TimeSlideEventList();
            eventsPerKey.put(groupKey, events);
        }
        
        long timestamp = System.currentTimeMillis();
        long expirtimestamp = timestamp + getKeepTime();
        events.add(expirtimestamp, theEvent);
        
        timegroup = timegroups.get(expirtimestamp);
        if (null == timegroup)
        {
            timegroup = new HashSet<Object>();
        }
        
        timegroup.add(groupKey);
        timegroups.put(expirtimestamp, timegroup);
        
        if (subCollection != null)
        {
            subCollection.update(new IEvent[] {theEvent}, null);
        }
        
        if (subViews instanceof IView)
        {
            IEvent[] newData = new IEvent[] {theEvent};
            ((IView)subViews).update(newData, null);
            
            LOG.debug("Send Batch Window Events For GroupID: {}.", groupKey);
        }
    }
    
}
