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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.view.IDataCollection;

/**
 * <根据索引访问窗口数据缓存集>
 * <缓存集保存窗口数据，并进行更新。可以通过索引信息定位窗口中数据，并返回。>
 * 
 */
public class WindowRandomAccess implements IDataCollection, IRandomAccessByIndex
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 559492793521282165L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(WindowRandomAccess.class);
    
    /**
     * 窗口事件
     */
    private final LinkedList<IEvent> events;
    
    /**
     * 根据索引访问窗口事件服务
     */
    private final RandomAccessByIndexService service;
    
    /**
     * <默认构造函数>
     *@param service 通过索引获取事件服务
     */
    public WindowRandomAccess(RandomAccessByIndexService service)
    {
        if (service == null)
        {
            String msg = "Random Access Service is Null.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        this.events = new LinkedList<IEvent>();
        this.service = service;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        if (service != null)
        {
            service.updated(this);
        }
        
        if (newData != null)
        {
            for (int i = 0; i < newData.length; i++)
            {
                events.addFirst(newData[i]);
            }
        }
        
        if (oldData != null)
        {
            for (int i = 0; i < oldData.length; i++)
            {
                events.remove(oldData[i]);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IEvent getEvent(int index, boolean isNew)
    {
        if (!isNew)
        {
            return null;
        }
        
        if (index < events.size())
        {
            return events.get(index);
        }
        
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IDataCollection renew()
    {
        return new WindowRandomAccess(service);
    }
    
}
