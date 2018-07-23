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
 * <Select中仅仅包含聚合操作，同时对于新事件不进行计算。>
 * 
 */
public class AggResultSetMergeOnlyGroupedExclude extends AggResultSetMergeOnlyGrouped
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 1499733893076804257L;
    
    /**
     * <默认构造函数>
     *@param aggregator 聚合操作
     *@param selector   选择操作
     *@param groupby    分组操作
     *@param order      排序操作
     *@param limit      限量操作
     */
    public AggResultSetMergeOnlyGroupedExclude(IAggregationService aggregator, SelectSubProcess selector,
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
        
        //对新旧事件计算分组键
        Map<Object, IEvent> keysAndEvents = new HashMap<Object, IEvent>();
        Object[] newDataMultiKey = generateGroupKeys(newData, keysAndEvents);
        Object[] oldDataMultiKey = generateGroupKeys(oldData, keysAndEvents);
        
        IEvent[] selectOldEvents = null;
        
        if (type != OutputType.I)
        {
            selectOldEvents = generateOutputEvents(keysAndEvents);
        }
        
        //处理聚合操作
        IEvent theEvent;
        if (oldData != null)
        {
            //对旧数据进行聚合操作
            for (int i = 0; i < oldData.length; i++)
            {
                theEvent = oldData[i];
                aggregator.processLeave(theEvent, oldDataMultiKey[i]);
            }
        }
        
        if (newData != null)
        {
            //对新数据进行聚合操作
            for (int i = 0; i < newData.length; i++)
            {
                aggregator.setAggregatorForKey(newDataMultiKey[i]);
            }
        }
        //处理select 语句，完成聚合操作值获取，如果其中有过滤的话，需要过滤。
        IEvent[] selectNewEvents = generateOutputEvents(keysAndEvents);
        
        if (newData != null)
        {
            //对新数据进行聚合操作
            for (int i = 0; i < newData.length; i++)
            {
                theEvent = newData[i];
                aggregator.processEnter(theEvent, newDataMultiKey[i]);
            }
        }
        
        //返回排序和限量后的结果集
        return orderAndLimit(selectNewEvents, selectOldEvents, type);
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Pair<IEvent[], IEvent[]> processJoinResult(Set<MultiKey> newEvents, Set<MultiKey> oldEvents, OutputType type)
    {
        IAggregationService aggregator = getAggregator();
        
        Map<Object, IEvent[]> keysAndEvents = new HashMap<Object, IEvent[]>();
        Object[] newDataGroupByKeys = generateGroupKeys(newEvents, keysAndEvents);
        Object[] oldDataGroupByKeys = generateGroupKeys(oldEvents, keysAndEvents);
        
        IEvent[] selectOldEvents = null;
        
        if (type != OutputType.I)
        {
            selectOldEvents = generateOutputEventsJoin(keysAndEvents);
        }
        
        if (oldEvents != null)
        {
            //对旧数据进行聚合操作
            int count = 0;
            for (MultiKey composed : oldEvents)
            {
                IEvent[] eventsPerStream = (IEvent[])composed.getKeys();
                aggregator.processEnter(eventsPerStream, oldDataGroupByKeys[count]);
                count++;
            }
        }
        
        if (newEvents != null)
        {
            //对新数据进行聚合操作
            for(int count = 0; count < newEvents.size(); count++)
            {
                aggregator.setAggregatorForKey(newDataGroupByKeys[count]);
            }
        }
        
        //处理select 语句，完成聚合操作值获取，如果其中有过滤的话，需要过滤。
        IEvent[] selectNewEvents = generateOutputEventsJoin(keysAndEvents);
        
        //处理聚合操作
        if (newEvents != null)
        {
            // 对新数据进行聚合操作
            int count = 0;
            for (MultiKey composed : newEvents)
            {
                IEvent[] eventsPerStream = (IEvent[])composed.getKeys();
                aggregator.processEnter(eventsPerStream, newDataGroupByKeys[count]);
                count++;
            }
        }
        
        //返回排序和限量后的结果集
        return orderAndLimit(selectNewEvents, selectOldEvents, type);
    }
    
}
