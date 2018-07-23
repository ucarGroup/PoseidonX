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

import java.util.LinkedHashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IRenew;
import com.huawei.streaming.view.IView;
import com.huawei.streaming.view.ViewImpl;

/**
 * <保留全部数据的窗口>
 * <上一个视图的旧数据，从窗口中删除，上一个视图的数据，加入窗口。>
 * 
 */
public class KeepAllWindow extends ViewImpl implements IWindow, IRenew
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 1168094719248524511L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(KeepAllWindow.class);
    
    /**
     * 窗口中保留事件
     */
    private LinkedHashSet<IEvent> events;
    
    /**
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;
    
    /**
     * <默认构造函数>
     *
     */
    public KeepAllWindow()
    {
        events = new LinkedHashSet<IEvent>();
    }
    
    /** {@inheritDoc} */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        if (newData == null && oldData == null)
        {
            return;
        }
        
        if (newData != null)
        {
            for (IEvent theEvent : newData)
            {
                events.add(theEvent);
            }
        }
        
        if (oldData != null)
        {
            for (IEvent theEvent : oldData)
            {
                events.remove(theEvent);
            }
        }
        
        if (dataCollection != null)
        {
            dataCollection.update(newData, oldData);
        }
        
        updateChild(newData, oldData);
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IView renewView()
    {
        KeepAllWindow renewWindow = new KeepAllWindow();
        IDataCollection renewDataCollection = dataCollection.renew();
        renewWindow.setDataCollection(renewDataCollection);
        return renewWindow;
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
    
}
