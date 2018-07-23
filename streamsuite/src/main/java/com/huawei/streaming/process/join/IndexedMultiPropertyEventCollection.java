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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.EventUtils;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.expression.PropertyValueExpression;

/**
 * 
 * 根据ON条件中的流的表达式进行有索引的窗口有效数据保存
 * <功能详细描述>
 * 
 */
public class IndexedMultiPropertyEventCollection implements IEventCollection, IIndexedEventCollection
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 5599399538292634925L;
    
    private static final Logger LOG = LoggerFactory.getLogger(IndexedMultiPropertyEventCollection.class);
    
    private final PropertyValueExpression[] propertyExpr;
    
    private String streamName;
    
    private IEventType eventType;
    
    private Map<MultiKey, Set<IEvent>> keyBasedEvent;
    
    /**
     * <默认构造函数>
     *@param name 流名称
     *@param type 事件类型
     *@param exp 属性值表达式
     */
    public IndexedMultiPropertyEventCollection(String name, IEventType type, final PropertyValueExpression[] exp)
    {
        LOG.debug("initiate IndexedExpressionEventCollection. streamName={}", name);
        this.streamName = name;
        this.eventType = type;
        this.propertyExpr = exp.clone();
        keyBasedEvent = new HashMap<MultiKey, Set<IEvent>>();
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
        for (IEvent theEvent : events)
        {
            add(theEvent);
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
        for (IEvent theEvent : events)
        {
            remove(theEvent);
        }
        
    }
    
    private void add(IEvent event)
    {
        if (null == event)
        {
            return;
        }
        
        MultiKey key = EventUtils.getMultiKey(event, propertyExpr);
        
        Set<IEvent> events = keyBasedEvent.get(key);
        
        if (events == null)
        {
            events = new LinkedHashSet<IEvent>();
            keyBasedEvent.put(key, events);
        }
        events.add(event);
    }
    
    private void remove(IEvent event)
    {
        if (null == event)
        {
            return;
        }
        
        MultiKey key = EventUtils.getMultiKey(event, propertyExpr);
        
        Set<IEvent> events = keyBasedEvent.get(key);
        if (events == null)
        {
            return;
        }
        
        if (!events.remove(event))
        {
            // Not an error, its possible that an old-data event is artificial (such as for statistics) and
            // thus did not correspond to a new-data event raised earlier.
            return;
        }
        
        if (events.isEmpty())
        {
            keyBasedEvent.remove(key);
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
    
    @Override
    public String getStreamName()
    {
        return streamName;
    }
    
    /**
     * 查找与KEY相关的事件集合,集合可以为空
     * <功能详细描述>
     */
    public Set<IEvent> lookup(MultiKey key)
    {
        return keyBasedEvent.get(key);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public MultiKey getIndex(IEvent event)
    {
        return EventUtils.getMultiKey(event, propertyExpr);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<IEvent> lookupWithNull(MultiKey key)
    {
        Set<IEvent> result = keyBasedEvent.get(key);
        if (null == result || result.isEmpty())
        {
            result = new LinkedHashSet<IEvent>();
            result.add(genNullEvent());
        }
        return result;
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
