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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.view.IDataCollection;

/**
 * <根据事件和相对索引访问窗口数据缓存集>
 * <缓存集保存窗口数据，并进行更新。根据事件和相对索引访问窗口中数据，并返回。>
 * 
 */
public class WindowRelativeAccess implements IDataCollection, IRelativeAccessByEventAndIndex
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -5048702594114151904L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(WindowRelativeAccess.class);
    
    /**
     * 事件索引信息
     */
    private final Map<IEvent, Integer> indexPerEvent;
    
    /**
     * 窗口数据
     */
    private IEvent[] events;
    
    /**
     * 访问窗口事件服务
     */
    private final RelativeAccessByEventAndIndexService service;
    
    /**
     * <默认构造函数>
     *@param service 通过事件和相对索引获取事件服务
     */
    public WindowRelativeAccess(RelativeAccessByEventAndIndexService service)
    {
        if (service == null)
        {
            String msg = "Relative Access Service is Null.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        this.service = service;
        indexPerEvent = new HashMap<IEvent, Integer>();
    }
    
    /** {@inheritDoc} */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        if (service != null)
        {
            service.updated(this);
        }
        indexPerEvent.clear();
        events = newData;
        
        if (newData != null)
        {
            for (int i = 0; i < newData.length; i++)
            {
                indexPerEvent.put(newData[i], i);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IEvent getEvent(IEvent theEvent, int index)
    {
        if (events == null)
        {
            return null;
        }
        
        if (index == 0)
        {
            return theEvent;
        }
        
        Integer indexIncoming = indexPerEvent.get(theEvent);
        if (indexIncoming == null)
        {
            return null;
        }
        
        if (index > indexIncoming)
        {
            return null;
        }
        
        int relativeIndex = indexIncoming - index;
        if ((relativeIndex < events.length) && (relativeIndex >= 0))
        {
            return events[relativeIndex];
        }
        return null;
    }
    
    /** {@inheritDoc} */
    @Override
    public IDataCollection renew()
    {
        return new WindowRelativeAccess(service);
    }
}
