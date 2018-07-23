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

import java.util.ArrayDeque;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;

/**
 * <基于长度滑动的分组窗口>
 * <对于每个分组值都有对应的事件保存数据结构，当该分组值对应的数据大于保存长度时，发送给子视图。不会影响其它分组值的处理>
 * 
 */
public class GroupLengthSlideWindow extends GroupLengthBasedWindow
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 2865946154722522439L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(GroupLengthSlideWindow.class);
    
    /**
     * 分组窗口中每个分组对应事件 集合
     */
    private final HashMap<Object, Object> eventsPerKey = new HashMap<Object, Object>();
    
    /**
     * 事件
     */
    private ArrayDeque<IEvent> events = null;
    
    /**
     * <默认构造函数>
     */
    public GroupLengthSlideWindow(IExpression[] exprs, int keepLength)
    {
        super(exprs, keepLength);
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    protected void processGroupedEvent(Object subViews, IDataCollection subCollection, Object groupKey, IEvent theEvent)
    {
        events = (ArrayDeque<IEvent>)this.eventsPerKey.get(groupKey);
        
        if (null == events)
        {
            events = new ArrayDeque<IEvent>();
            eventsPerKey.put(groupKey, events);
        }
        
        events.add(theEvent);
        
        LOG.debug("The newData has been added to LengthSlideWindow.");
        
        IEvent[] newData = new IEvent[] {theEvent};
        IEvent[] oldData = null;
        
        /**
         * 判断窗口中事件是否大于窗口保持事件长度，
         * 将多余的事件标记为过期时间，移出窗口。
         */
        
        int expireCount = (int)(events.size() - getKeepLength());
        if (expireCount > 0)
        {
            oldData = new IEvent[expireCount];
            for (int i = 0; i < expireCount; i++)
            {
                oldData[i] = events.removeFirst();
            }
        }
        
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
            
            LOG.debug("Send Slide Window Events For GroupID: {}.", groupKey);
        }
    }
    
}
