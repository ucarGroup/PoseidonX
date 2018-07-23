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

package com.huawei.streaming.process.join;

import java.util.LinkedHashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;

/**
 * 
 * 没有索引的待JOIN窗口有效事件保存
 * <功能详细描述>
 * 
 */
public class SimpleEventCollection implements IEventCollection
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 656105706993754245L;
    
    private static final Logger LOG = LoggerFactory.getLogger(SimpleEventCollection.class);
    
    private String streamName;
    
    private Set<IEvent> keyBasedEvent;
    
    private IEventType eventType;
    
    /**
     * <默认构造函数>
     *@param name 保存的数据流名称
     *@param type 事件类型
     */
    public SimpleEventCollection(String name, IEventType type)
    {
        LOG.debug("Init SimpleEventCollection(). stream name={}.", name);
        this.streamName = name;
        this.eventType = type;
        keyBasedEvent = new LinkedHashSet<IEvent>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void addRemove(IEvent[] newData, IEvent[] oldData)
    {
        add(newData);
        remove(oldData);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void add(IEvent[] events)
    {
        if (events == null)
        {
            return;
        }
        for (IEvent e : events)
        {
            keyBasedEvent.add(e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(IEvent[] events)
    {
        if (events == null)
        {
            return;
        }
        for (IEvent e : events)
        {
            keyBasedEvent.remove(e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty()
    {
        return keyBasedEvent.isEmpty();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        keyBasedEvent.clear();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getStreamName()
    {
        return this.streamName;
    }
    
    /**
     * 获得所有保存的事件（窗口有效事件）
     * <功能详细描述>
     */
    public Set<IEvent> lookupAll()
    {
        return keyBasedEvent;
    }
    
    /**
     * 如果当前保存结果为空，返回一个含有一个所有值为空的事件集合
     * <功能详细描述>
     */
    public Set<IEvent> lookupAllWithNull()
    {
        if (keyBasedEvent.isEmpty())
        {
            Set<IEvent> nullEvent = new LinkedHashSet<IEvent>();
            nullEvent.add(genNullEvent());
            return nullEvent;
        }
        return keyBasedEvent;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IEvent genNullEvent()
    {
        Attribute[] atts = this.eventType.getAllAttributes();
        Object[] values = new Object[atts.length];
        return new TupleEvent(this.streamName, this.eventType, values);
    }
    
}
