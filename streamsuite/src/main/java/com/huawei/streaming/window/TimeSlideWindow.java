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

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.timeservice.TimeService;
import com.huawei.streaming.view.IDataCollection;

/**
 * <时间滑动窗口实现类>
 * 
 */
public class TimeSlideWindow extends TimeBasedWindow
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 1444510944409525421L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimeSlideWindow.class);
    
    /**
     * 窗口事件保存对象，记录时间与事件的索引方便得到过期事件
     */
    private TimeSlideEventList events = new TimeSlideEventList();
    
    /**
     * <默认构造函数>
     *@param keepTime 窗口保持时间，单位：ms
     *@param slideInterval 窗口滑动间隔，单位：ms
     */
    public TimeSlideWindow(long keepTime, long slideInterval)
    {
        super(keepTime);
        
        TimeService timeservice = new TimeService(slideInterval, this);
        setTimeservice(timeservice);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        //TODO 未考虑窗口叠加时，上一个窗口传递的过期数据的处理。
        
        if ((null == newData) || (0 == newData.length))
        {
            LOG.error("Input Time Slide Window newData is Null!");
            return;
        }
        
        try
        {
            lock();
            //TODO 存在时间漂移的问题，会导致数据顺序不准确。
            long timestamp = System.currentTimeMillis();
            //TODO 是否需要考虑窗口中的事件过期，当前处理认为事件过期已经通过定时处理，来新事件时，不存在过期数据。
            for (int i = 0; i < newData.length; i++)
            {
                events.add(timestamp + getKeepTime(), newData[i]);
            }
            
            IDataCollection dataCollection = getDataCollection();
            if (dataCollection != null)
            {
                dataCollection.update(newData, null);
            }
            
            if (this.hasViews())
            {
                updateChild(newData, null);
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
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void timerCallBack(long currentTime)
    {
        try
        {
            lock();
            IEvent[] oldData = events.getOldData(currentTime);
            
            if (null == oldData)
            {
                return;
            }
            else
            {
                IDataCollection dataCollection = getDataCollection();
                if (dataCollection != null)
                {
                    dataCollection.update(null, oldData);
                }
                
                if (this.hasViews())
                {
                    updateChild(null, oldData);
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
    
}
