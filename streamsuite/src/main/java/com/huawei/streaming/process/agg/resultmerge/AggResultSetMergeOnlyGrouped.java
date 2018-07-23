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

package com.huawei.streaming.process.agg.resultmerge;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.GroupBySubProcess;
import com.huawei.streaming.process.LimitProcess;
import com.huawei.streaming.process.OrderBySubProcess;
import com.huawei.streaming.process.SelectSubProcess;
import com.huawei.streaming.process.agg.compute.IAggregationService;

/**
 * <分组聚合操作的结果合并处理类,支持select中仅包含聚合操作，或者包含聚合操作以及group by对应的属性字段>
 * select sum(a) from stream.lengthbatchwindow(10) group by b;
 * select b, sum(a) from stream.lengthbatchwindow(10) group by b;>
 * 
 */
public class AggResultSetMergeOnlyGrouped extends AggResultSetMergeGrouped
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 1996529928367579227L;
    
    /**
     * <默认构造函数>
     *@param aggregator 聚合操作
     *@param selector   选择操作
     *@param groupby    分组操作
     *@param order      排序操作
     *@param limit      限量操作
     */
    public AggResultSetMergeOnlyGrouped(IAggregationService aggregator, SelectSubProcess selector,
        GroupBySubProcess groupby, OrderBySubProcess order, LimitProcess limit)
    {
        super(aggregator, selector, groupby, order, limit);
    }
    
    /**
     * {@inheritDoc}
     */
    public Pair<IEvent[], IEvent[]> processResult(IEvent[] newData, IEvent[] oldData, OutputType type)
    {
        IAggregationService aggregator = getAggregator();
        
        if (getUnidirection())
        {
            aggregator.clearResults();
        }
        
        //对新旧事件计算分组键
        Map<Object, IEvent> keysAndEvents = new HashMap<Object, IEvent>();
        Object[] newDataMultiKey = generateGroupKeys(newData, keysAndEvents);
        Object[] oldDataMultiKey = generateGroupKeys(oldData, keysAndEvents);
        
        IEvent[] selectOldEvents = null;
        
        if (type != OutputType.I)
        {
            selectOldEvents = generateOutputEvents(keysAndEvents);
        }
        
        processAggregateData(newData, oldData, newDataMultiKey, oldDataMultiKey);
        
        //处理select 语句，完成聚合操作值获取，如果其中有过滤的话，需要过滤。
        IEvent[] selectNewEvents = generateOutputEvents(keysAndEvents);
        
        //返回排序和限量后的结果集
        return orderAndLimit(selectNewEvents, selectOldEvents, type);
        
    }
    
    /**
     * <产生分组健，并保存分组健和事件对应关系>
     */
    protected Object[] generateGroupKeys(IEvent[] events, Map<Object, IEvent> keysAndEvents)
    {
        if (events == null)
        {
            return null;
        }
        
        IEvent eventsPerStream;
        Object keys[] = new Object[events.length];
        
        //计算分组键并保存
        for (int i = 0; i < events.length; i++)
        {
            eventsPerStream = events[i];
            keys[i] = generateGroupKey(eventsPerStream);
            keysAndEvents.put(keys[i], events[i]);
        }
        
        return keys;
    }
    
    /**
     * <产生结果集>
     */
    protected IEvent[] generateOutputEvents(Map<Object, IEvent> keysAndEvents)
    {
        IAggregationService aggregator = getAggregator();
        SelectSubProcess selector = getSelector();
        
        //每个分组键产生一条结果
        int count = 0;
        IEvent[] events = new IEvent[keysAndEvents.size()];
        IEvent theEvent;
        
        for (Map.Entry<Object, IEvent> entry : keysAndEvents.entrySet())
        {
            //设置当前聚合操作对象
            aggregator.setCurrentAggregator(entry.getKey());
            
            //根据当前事件和聚合操作对象，得到结果
            theEvent = entry.getValue();
            events[count] = selector.processSingle(theEvent);
            count++;
        }
        
        return events;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Pair<IEvent[], IEvent[]> processJoinResult(Set<MultiKey> newEvents, Set<MultiKey> oldEvents, OutputType type)
    {
        IAggregationService aggregator = getAggregator();
        
        if (getUnidirection())
        {
            aggregator.clearResults();
        }
        
        Map<Object, IEvent[]> keysAndEvents = new HashMap<Object, IEvent[]>();
        Object[] newDataGroupByKeys = generateGroupKeys(newEvents, keysAndEvents);
        Object[] oldDataGroupByKeys = generateGroupKeys(oldEvents, keysAndEvents);
        
        IEvent[] selectOldEvents = null;
        
        if (type != OutputType.I)
        {
            selectOldEvents = generateOutputEventsJoin(keysAndEvents);
        }
        
        processJoinAggregateData(newEvents, oldEvents, newDataGroupByKeys, oldDataGroupByKeys);
        
        //处理select 语句，完成聚合操作值获取，如果其中有过滤的话，需要过滤。
        IEvent[] selectNewEvents = generateOutputEventsJoin(keysAndEvents);
        
        //返回排序和限量后的结果集
        return orderAndLimit(selectNewEvents, selectOldEvents, type);
    }
    
    /**
     * <根据JOIN事件产生结果>
     * <功能详细描述>
     */
    protected IEvent[] generateOutputEventsJoin(Map<Object, IEvent[]> keysAndEvents)
    {
        IEvent[] events = new IEvent[keysAndEvents.size()];
        
        IAggregationService aggregator = getAggregator();
        SelectSubProcess selector = getSelector();
        
        //每条JOIN事件产生结果
        int count = 0;
        for (Map.Entry<Object, IEvent[]> entry : keysAndEvents.entrySet())
        {
            //根据分组健，设置当前聚合操作对象
            aggregator.setCurrentAggregator(entry.getKey());
            IEvent[] eventsPerStream = entry.getValue();
            
            //根据事件和当前聚合操作对象，得到结果
            events[count] = (IEvent)selector.processSingle(eventsPerStream);
            count++;
        }
        
        return events;
    }
    
    /**
     * <产生分组健，并保存分组健和事件对应关系>
     */
    protected Object[] generateGroupKeys(Set<MultiKey> eventSet, Map<Object, IEvent[]> keysAndEvents)
    {
        if (eventSet == null || eventSet.isEmpty())
        {
            return null;
        }
        
        Object keys[] = new Object[eventSet.size()];
        
        int count = 0;
        for (MultiKey composed : eventSet)
        {
            IEvent[] eventsPerStream = (IEvent[])composed.getKeys();
            keys[count] = generateGroupKey(eventsPerStream);
            keysAndEvents.put(keys[count], eventsPerStream);
            
            count++;
        }
        
        return keys;
    }
}
