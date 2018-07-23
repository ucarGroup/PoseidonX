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

import java.util.Set;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.GroupBySubProcess;
import com.huawei.streaming.process.LimitProcess;
import com.huawei.streaming.process.OrderBySubProcess;
import com.huawei.streaming.process.SelectSubProcess;
import com.huawei.streaming.process.agg.compute.IAggregationService;

/**
 * <分组属性和聚合操作的结果合并处理类>
 * <对每个分组完成聚合操作计算，将聚合操作结果和属性值进行合并得到结果集，对结果集进行排序和限量后输出。>
 * 
 */
public class AggResultSetMergeGrouped extends ResultSetMergeImpl
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 5731952621452545763L;
    
    /**
     * 聚合操作计算处理对象
     */
    private final IAggregationService aggregator;
    
    /**
     * 输出结果合并处理对象
     */
    private final SelectSubProcess selector;
    
    /**
     * 分组处理对象
     */
    private final GroupBySubProcess groupby;
    
    /**
     * <默认构造函数>
     *@param aggregator 聚合操作
     *@param selector   选择操作
     *@param groupby    分组操作
     *@param order      排序操作
     *@param limit      限量操作
     */
    public AggResultSetMergeGrouped(IAggregationService aggregator, SelectSubProcess selector,
        GroupBySubProcess groupby, OrderBySubProcess order, LimitProcess limit)
    {
        super(order, limit);
        this.aggregator = aggregator;
        this.selector = selector;
        this.groupby = groupby;
    }
    
    /**
     * {@inheritDoc}
     */
    public Pair<IEvent[], IEvent[]> processResult(IEvent[] newData, IEvent[] oldData, OutputType type)
    {
        //对新旧事件计算分组键
        Object[] newDataGroupByKeys = generateGroupKeys(newData);
        Object[] oldDataGroupByKeys = generateGroupKeys(oldData);
        
        processAggregateData(newData, oldData, newDataGroupByKeys, oldDataGroupByKeys);
        
        //处理select 语句，完成属性表达式和聚合操作值获取，如果其中有过滤的话，需要过滤。
        IEvent[] selectOldEvents = null;
        if (type != OutputType.I)
        {
            selectOldEvents = generateOutputEvents(oldData, oldDataGroupByKeys);
        }
        IEvent[] selectNewEvents = generateOutputEvents(newData, newDataGroupByKeys);
        
        //返回排序和限量后的结果集
        return orderAndLimit(selectNewEvents, selectOldEvents, type);
    }

    /**
     * 对数据进行分组聚合表达式处理
     * 
     */
    protected void processAggregateData(IEvent[] newData, IEvent[] oldData, Object[] newDataGroupByKeys,
        Object[] oldDataGroupByKeys)
    {
        //处理聚合操作
        if (newData != null)
        {
            //对新数据进行聚合操作
            for (int i = 0; i < newData.length; i++)
            {
                aggregator.processEnter(newData[i], newDataGroupByKeys[i]);
            }
        }
        if (oldData != null)
        {
            //对旧数据进行聚合操作
            for (int i = 0; i < oldData.length; i++)
            {
                aggregator.processLeave(oldData[i], oldDataGroupByKeys[i]);
            }
        }
    }
    
    /**
     * <产生分组健>
     */
    private Object[] generateGroupKeys(IEvent[] events)
    {
        if (events == null)
        {
            return null;
        }
        
        IEvent eventsPerStream;
        Object keys[] = new Object[events.length];
        
        //对每条事件产生对应的分组健
        for (int i = 0; i < events.length; i++)
        {
            eventsPerStream = events[i];
            keys[i] = generateGroupKey(eventsPerStream);
        }
        
        return keys;
    }
    
    /**
     * <产生事件分组健>
     */
    protected Object generateGroupKey(IEvent theEvent)
    {
        Object[] keys = new Object[groupby.getGroupKeyExprs().length];
        
        // 对分组表达式求值
        int count = 0;
        for (IExpression exprNode : groupby.getGroupKeyExprs())
        {
            keys[count] = exprNode.evaluate(theEvent);
            count++;
        }
        
        return new MultiKey(keys);
    }
    
    /**
     * <属性表达式和聚合操作值获取>
     */
    private IEvent[] generateOutputEvents(IEvent[] data, Object[] groupByKeys)
    {
        if (data == null)
        {
            return null;
        }
        
        IEvent[] events = new IEvent[data.length];
        
        //每条事件产生结果
        for (int i = 0; i < data.length; i++)
        {
            //根据分组健，设置当前聚合操作对象
            aggregator.setCurrentAggregator(groupByKeys[i]);
            IEvent theEvent = data[i];
            
            //根据事件和当前聚合操作对象，得到结果
            events[i] = (IEvent)selector.processSingle(theEvent);
        }
        
        return events;
    }
    
    /**
     * <返回聚合操作>
     */
    protected IAggregationService getAggregator()
    {
        return aggregator;
    }
    
    /**
     * <返回选择操作>
     */
    protected SelectSubProcess getSelector()
    {
        return selector;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Pair<IEvent[], IEvent[]> processJoinResult(Set<MultiKey> newEvents, Set<MultiKey> oldEvents, OutputType type)
    {
        //对新旧事件计算分组键
        Object[] newDataGroupByKeys = generateGroupKeys(newEvents);
        Object[] oldDataGroupByKeys = generateGroupKeys(oldEvents);
        
        processJoinAggregateData(newEvents, oldEvents, newDataGroupByKeys, oldDataGroupByKeys);
        
        //处理select 语句，完成属性表达式和聚合操作值获取，如果其中有过滤的话，需要过滤。
        IEvent[] selectOldEvents = null;
        if (type != OutputType.I)
        {
            selectOldEvents = generateOutputEventsJoin(oldEvents, oldDataGroupByKeys);
        }
        IEvent[] selectNewEvents = generateOutputEventsJoin(newEvents, newDataGroupByKeys);
        
        //返回排序和限量后的结果集
        return orderAndLimit(selectNewEvents, selectOldEvents, type);
    }

    /**
     * 对Join数据进行分组聚合表达式处理
     * 
     */
    protected void processJoinAggregateData(Set<MultiKey> newEvents, Set<MultiKey> oldEvents,
        Object[] newDataGroupByKeys, Object[] oldDataGroupByKeys)
    {
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
    }
    
    private Object[] generateGroupKeys(Set<MultiKey> eventSet)
    {
        if (eventSet.isEmpty())
        {
            return null;
        }
        
        Object keys[] = new Object[eventSet.size()];
        
        int count = 0;
        for (MultiKey composed : eventSet)
        {
            IEvent[] eventsPerStream = (IEvent[])composed.getKeys();
            keys[count] = generateGroupKey(eventsPerStream);
            count++;
        }
        
        return keys;
    }
    
    /**
     * <根据JOIN后事件集合，得到分组键值>
     * <根据JOIN后事件集合，得到分组键值>
     */
    protected Object generateGroupKey(IEvent[] eventsPerStream)
    {
        Object[] keys = new Object[groupby.getGroupKeyExprs().length];
        
        // 对分组表达式求值
        int count = 0;
        for (IExpression exprNode : groupby.getGroupKeyExprs())
        {
            keys[count] = exprNode.evaluate(eventsPerStream);
            count++;
        }
        
        return new MultiKey(keys);
    }
    
    private IEvent[] generateOutputEventsJoin(Set<MultiKey> eventSet, Object[] groupByKeys)
    {
        if (eventSet.isEmpty())
        {
            return null;
        }
        IEvent[] events = new IEvent[eventSet.size()];
        
        //每条JOIN事件产生结果
        int count = 0;
        for (MultiKey composed : eventSet)
        {
            //根据分组健，设置当前聚合操作对象
            aggregator.setCurrentAggregator(groupByKeys[count]);
            IEvent[] eventsPerStream = (IEvent[])composed.getKeys();
            
            //根据事件和当前聚合操作对象，得到结果
            events[count] = (IEvent)selector.processSingle(eventsPerStream);
            count++;
        }
        
        return events;
    }
    
}
