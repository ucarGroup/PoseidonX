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

import java.util.LinkedList;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;

/**
 * <基于事件中时间戳属性驱动的时间跳动窗口>
 * <根据事件中的时间戳信息，判断窗口中的最旧时间和新时间的差值是否大于窗口时间间隔。
 *  如果大于窗口时间间隔，则窗口中事件作为新事件发出，并作为下次的旧事件；否则将新事件保存在窗口中。>
 * 
 */
public class EventTimeBatchWindow extends EventTimeBasedWindow
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -5650779735483325548L;
    
    private LinkedList<IEvent> datas = new LinkedList<IEvent>();
    
    private Long oldestTime = null;
    
    private IEvent[] lastBatch = null;
    
    /**
     * <默认构造函数>
     *@param keepTime 窗口保持时间
     *@param timeExpr 时间属性表达式
     */
    public EventTimeBatchWindow(long keepTime, IExpression timeExpr)
    {
        super(keepTime, timeExpr);
    }
    
    /** {@inheritDoc} */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        if (oldData != null && oldData.length > 0)
        {
            for (IEvent theEvent : oldData)
            {
                datas.remove(theEvent);
            }
            
            calcWindowOldestTime();
        }
        
        IEvent[] curBatch = null;
        if (newData != null)
        {
            
            for (IEvent theEvent : newData)
            {
                long timestamp = getTimestamp(theEvent);
                
                if (oldestTime == null)
                {
                    oldestTime = timestamp;
                }
                else
                {
                    if (timestamp - oldestTime >= getKeepTime())
                    {
                        if (curBatch == null)
                        {
                            curBatch = datas.toArray(new IEvent[datas.size()]);
                        }
                        else
                        {
                            IEvent[] tempArray = new IEvent[curBatch.length + datas.size()];
                            System.arraycopy(curBatch, 0, tempArray, 0, curBatch.length);
                            
                            int counter = curBatch.length;
                            for (IEvent event : datas)
                            {
                                tempArray[counter++] = event;
                            }
                            
                            curBatch = tempArray;
                        }
                        datas.clear();
                        oldestTime = null;
                    }
                }
                
                datas.add(theEvent);
                if (oldestTime == null)
                {
                    oldestTime = timestamp;
                }
            }
        }
        
        IDataCollection dataCollection = getDataCollection();
        if (curBatch != null)
        {
            if (dataCollection != null)
            {
                dataCollection.update(curBatch, lastBatch);
            }
            
            updateChild(curBatch, lastBatch);
            lastBatch = curBatch;
        }
        
        if (oldData != null && oldData.length > 0)
        {
            if (dataCollection != null)
            {
                dataCollection.update(null, oldData);
            }
            
            updateChild(null, oldData);
        }
    }
    
    /**
     * <计算当前窗口中最老事件时间信息>
     * <功能详细描述>
     */
    private void calcWindowOldestTime()
    {
        oldestTime = null;
        
        if (!datas.isEmpty())
        {
            oldestTime = getTimestamp(datas.getFirst());
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IView renewView()
    {
        EventTimeBatchWindow renewWindow = new EventTimeBatchWindow(getKeepTime(), getTimeExpr());
        
        IDataCollection dataCollection = getDataCollection();
        if (dataCollection != null)
        {
            IDataCollection renewCollection = dataCollection.renew();
            renewWindow.setDataCollection(renewCollection);
        }
        
        return renewWindow;
    }
    
}
