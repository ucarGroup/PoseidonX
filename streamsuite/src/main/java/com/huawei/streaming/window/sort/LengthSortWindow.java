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

package com.huawei.streaming.window.sort;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.CollectionUtil;
import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.process.sort.MultiKeyComparator;
import com.huawei.streaming.process.sort.ObjectComparator;
import com.huawei.streaming.process.sort.ProxyComparator;
import com.huawei.streaming.process.sort.SortCondition;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;
import com.huawei.streaming.window.LengthBasedWindow;

/**
 * <基于长度的比较窗口>
 * <对事件进行表达式求值进行比较，值小的会先移出窗口，相等的将较老事件移出窗口。上一个窗口的旧事件会从当前窗口移出。>
 * 
 */
public class LengthSortWindow extends LengthBasedWindow
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 6806498086973329665L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(LengthSortWindow.class);
    
    /**
     * 排序条件，支持多个属性排序，按照链表里面的顺序为优先级进行排序
     */
    private List<SortCondition> sortConditions;
    
    /**
     * 窗口排序事件
     */
    private TreeMap<Object, Object> sortedEvents;
    
    /**
     * 事件数量
     */
    private int eventCount;
    
    /**
     * <默认构造函数>
     *@param keepLength 窗口保持长度
     *@param sortConditions 排序条件
     */
    public LengthSortWindow(int keepLength, final List<SortCondition> sortConditions)
    {
        super(keepLength);
        
        if (null == sortConditions || sortConditions.isEmpty())
        {
            String msg = "Sort condition is null.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        this.sortConditions = sortConditions;
        sortedEvents = new TreeMap<Object, Object>(getComparator(this.sortConditions));
    }
    
    /**
     * <得到比较器>
     */
    private static Comparator<Object> getComparator(final List<SortCondition> sortConditions)
    {
        //当排序条件大于1时，返回多值比较代理类，否则返回单值比较类
        if (sortConditions.size() > 1)
        {
            return new ProxyComparator(new MultiKeyComparator(sortConditions));
        }
        else
        {
            return new ObjectComparator(sortConditions.get(0));
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        ArrayDeque<IEvent> removedEvents = null;
        //处理旧事件，如果排序窗口中有旧事件，则将旧事件移出窗口。如果排序窗口中无旧事件，则说明已经移出窗口。
        if (oldData != null)
        {
            for (int i = 0; i < oldData.length; i++)
            {
                IEvent event = oldData[i];
                Object sortValues = getSortValues(event);
                boolean result = CollectionUtil.removeEventByKey(sortValues, event, sortedEvents);
                if (result)
                {
                    eventCount--;
                    if (removedEvents == null)
                    {
                        removedEvents = new ArrayDeque<IEvent>();//new EventCollection();
                    }
                    removedEvents.add(event);
                }
            }
        }
        
        //处理新事件，将新事件加入到排序窗口中
        if (newData != null)
        {
            for (int i = 0; i < newData.length; i++)
            {
                IEvent event = newData[i];
                Object sortValues = getSortValues(event);
                CollectionUtil.addEventByKeyIntoFront(sortValues, event, sortedEvents);
                eventCount++;
            }
        }
        
        //判断窗口中事件个数是否大于窗口保留长度，如果大于则将最小的事件移出窗口
        int keepLength = getKeepLength();
        if (eventCount > keepLength)
        {
            int removeCount = eventCount - keepLength;
            
            //数据移出规则为将排序窗口中最后元素移出，最后元素为队列的话，则根据进入窗口顺序移出(先入先出)
            for (int i = 0; i < removeCount; i++)
            {
                Object lastKey = sortedEvents.lastKey();
                Object lastValue = sortedEvents.get(lastKey);
                
                if (lastValue instanceof List)
                {
                    @SuppressWarnings("unchecked")
                    List<IEvent> events = (List<IEvent>)lastValue;
                    eventCount--;
                    IEvent event = events.remove(events.size() - 1);
                    if (events.isEmpty())
                    {
                        sortedEvents.remove(lastKey);
                    }
                    
                    if (removedEvents == null)
                    {
                        removedEvents = new ArrayDeque<IEvent>();
                    }
                    removedEvents.add((IEvent)event);
                }
                else
                {
                    eventCount--;
                    sortedEvents.remove(lastKey);
                    if (removedEvents == null)
                    {
                        removedEvents = new ArrayDeque<IEvent>();
                    }
                    removedEvents.add((IEvent)lastValue);
                }
            }
        }
        
        if (this.hasViews())
        {
            IEvent[] expireData = null;
            if (removedEvents != null)
            {
                expireData = removedEvents.toArray(new IEvent[removedEvents.size()]);
            }
            
            IDataCollection dataCollection = getDataCollection();
            if (dataCollection != null)
            {
                dataCollection.update(newData, expireData);
            }
            
            updateChild(newData, expireData);
        }
    }
    
    /**
     * <求事件对应的排序键值对象>
     */
    private Object getSortValues(IEvent theEvent)
    {
        Object[] result = new Object[sortConditions.size()];
        int count = 0;
        //TODO 当前排序条件均为属性，后续可以考虑其它表达式
        //根据排序属性得到属性值
        for (SortCondition condition : sortConditions)
        {
            result[count++] = theEvent.getValue(condition.getAttribute());
        }
        
        if (sortConditions.size() > 1)
        {
            return new MultiKey(result);
        }
        else
        {
            return result[0];
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IView renewView()
    {
        return new LengthSortWindow(getKeepLength(), sortConditions);
    }
    
}
