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

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;

/**
 * <基于事件中时间戳属性驱动的时间滑动窗口>
 * <根据事件中的时间戳信息，判断窗口中事件时间和新时间的差值是否大于窗口时间间隔。
 *  如果大于窗口时间间隔，则窗口中事件作为旧事件发出， 新事件作为新事件发出，且将新事件保存在窗口中。>
 * 
 */
public class EventTimeSlideWindow extends EventTimeBasedWindow
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -8781441821074235012L;
    
    /**
     * 窗口事件保存对象，记录时间与事件的索引方便得到过期事件
     */
    private TimeSlideEventList events = new TimeSlideEventList();
    
    /**
     * <默认构造函数>
     *@param keepTime 窗口保持时间
     *@param timeExpr 时间属性表达式
     */
    public EventTimeSlideWindow(long keepTime, IExpression timeExpr)
    {
        super(keepTime, timeExpr);
    }
    
    /** {@inheritDoc} */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        long timestamp = -1;
        if (newData != null)
        {
            for (int i = 0; i < newData.length; i++)
            {
                timestamp = getTimestamp(newData[i]);
                events.add(timestamp, newData[i]);
            }
        }
        
        IEvent[] expired = null;
        if (timestamp != -1)
        {
            expired = events.getOldData(timestamp - getKeepTime() + 1);
        }
        
        IEvent[] oldEvents = null;
        oldEvents = expired;
        
        /*if (oldData == null)
        {
            oldEvents = expired;
        }
        else
        {
            
            for (IEvent theOld : oldData)
            {
                events.remove(theOld);
            }
            
            if (oldEvents == null)
            {
                oldEvents = oldData;
            }
            else
            {
                oldEvents = CollectionUtil.addArrayAndDelDuplicate(oldData, oldEvents);
            }
        }*/
        
        IDataCollection dataCollection = getDataCollection();
        if (dataCollection != null)
        {
            dataCollection.update(newData, oldEvents);
        }
        
        if (this.hasViews())
        {
            updateChild(newData, oldEvents);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IView renewView()
    {
        EventTimeSlideWindow renewWindow = new EventTimeSlideWindow(getKeepTime(), getTimeExpr());
        
        IDataCollection dataCollection = getDataCollection();
        if (dataCollection != null)
        {
            IDataCollection renewCollection = dataCollection.renew();
            renewWindow.setDataCollection(renewCollection);
        }
        
        return renewWindow;
    }
}
