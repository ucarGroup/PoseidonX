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

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;
import com.huawei.streaming.window.TimeSlideEventList;

/**
 * <基于事件時間戳滑动的分组窗口>
 * <对于每个分组值都有对应的事件保存数据结构，当该分组值对应的窗口中，事件时间和新时间的差值是否大于窗口时间间隔。
 *  如果大于窗口时间间隔，则窗口中事件作为旧事件发出， 新事件作为新事件发出，且将新事件保存在窗口中。
 *  不会影响其它分组值的处理。>
 * 
 */
public class GroupEventTimeSlideWindow extends GroupEventTimeBasedWindow
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 2511117513024176117L;
    
    /**
     * 分组窗口中每个分组对应事件保存对象，记录时间与事件的索引方便得到过期事件
     */
    private HashMap<Object, TimeSlideEventList> eventPerKey;
    
    /**
     * <默认构造函数>
     *@param groupExprs 分組表達式
     *@param keepTime   窗口保存時間
     *@param timeExpr   時間戳表達式
     */
    public GroupEventTimeSlideWindow(IExpression[] groupExprs, long keepTime, IExpression timeExpr)
    {
        super(groupExprs, keepTime, timeExpr);
        
        eventPerKey = new HashMap<Object, TimeSlideEventList>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void processGroupedEvent(Object subViews, IDataCollection subCollection, Object groupKey, IEvent theEvent)
    {
        long timestamp = -1;
        TimeSlideEventList curEvents = eventPerKey.get(groupKey);
        
        if (curEvents == null)
        {
            curEvents = new TimeSlideEventList();
            eventPerKey.put(groupKey, curEvents);
        }
        timestamp = getTimestamp(theEvent);
        curEvents.add(timestamp, theEvent);
        
        IEvent[] newData = new IEvent[] {theEvent};
        IEvent[] expired = null;
        if (timestamp != -1)
        {
            expired = curEvents.getOldData(timestamp - getKeepTime() + 1);
        }
        
        IEvent[] oldData = expired;
        
        if (subCollection != null)
        {
            subCollection.update(newData, oldData);
        }
        
        if (null != subViews)
        {
            if (subViews instanceof IView)
            {
                ((IView)subViews).update(newData, oldData);
            }
        }
    }
    
}
