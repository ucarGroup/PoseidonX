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
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.GroupBySubProcess;
import com.huawei.streaming.process.LimitProcess;
import com.huawei.streaming.process.OrderBySubProcess;
import com.huawei.streaming.process.SelectSubProcess;
import com.huawei.streaming.process.agg.compute.IAggregationService;

/**
 * <属性和聚合操作的结果合并处理类>
 * <完成聚合操作计算，将聚合操作结果和属性值进行合并得到结果集，对结果集进行排序和限量后输出。>
 * 
 */
public class AggResultSetMerge extends ResultSetMergeImpl
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -7861249141426199600L;
    
    /**
     * 聚合操作计算处理对象
     */
    private final IAggregationService aggregator;
    
    /**
     * 输出结果合并处理对象
     */
    private final SelectSubProcess selector;
    
    /**
     * <默认构造函数>
     *@param aggregator 聚合操作
     *@param selector   选择操作
     *@param groupby    分组操作
     *@param order      排序操作
     *@param limit      限量操作
     */
    public AggResultSetMerge(IAggregationService aggregator, SelectSubProcess selector, GroupBySubProcess groupby,
        OrderBySubProcess order, LimitProcess limit)
    {
        super(order, limit);
        this.selector = selector;
        this.aggregator = aggregator;
    }
    
    /**
     * {@inheritDoc}
     */
    public Pair<IEvent[], IEvent[]> processResult(IEvent[] newData, IEvent[] oldData, OutputType type)
    {
        IEvent[] selectOldEvents = null;
        IEvent[] selectNewEvents;
        
        processAggregateData(newData, oldData);
        
        //处理select 语句，完成属性表达式和聚合操作值获取，如果其中有过滤的话，需要继续拧过滤。
        if (type != OutputType.I)
        {
            selectOldEvents = selector.process(oldData);
        }
        selectNewEvents = selector.process(newData);
        
        //返回排序和限量后的结果集
        return orderAndLimit(selectNewEvents, selectOldEvents, type);
    }

    /**
     * 处理聚合表达式
     * 新数据加入聚合计算，旧数据移除聚合计算
     */
    protected void processAggregateData(IEvent[] newData, IEvent[] oldData)
    {
        // 处理聚合操作
        if (newData != null)
        {
            // 对新数据进行聚合操作
            for (IEvent aNewData : newData)
            {
                aggregator.processEnter(aNewData, null);
            }
        }
        if (oldData != null)
        {
            // 对旧数据进行聚合操作
            for (IEvent anOldData : oldData)
            {
                aggregator.processLeave(anOldData, null);
            }
        }
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
    public Pair<IEvent[], IEvent[]> processJoinResult(Set<MultiKey> newEvents, Set<MultiKey> oldEvents, OutputType type)
    {
        IEvent[] selectOldEvents = null;
        IEvent[] selectNewEvents;
        
        if (getUnidirection())
        {
            aggregator.clearResults();
        }
        
        processJoinAggregateData(newEvents, oldEvents);
        
        //处理select 语句，完成属性表达式和聚合操作值获取，如果其中有过滤的话，需要继续拧过滤。
        if (type != OutputType.I)
        {
            selectOldEvents = selector.process(oldEvents);
        }
        selectNewEvents = selector.process(newEvents);
        
        //返回排序和限量后的结果集
        return orderAndLimit(selectNewEvents, selectOldEvents, type);
    }

    /**
     * 对Join结果进行聚合表达式处理
     * 新数据加入聚合计算，旧数据移除聚合计算
     */
    protected void processJoinAggregateData(Set<MultiKey> newEvents, Set<MultiKey> oldEvents)
    {
        // 处理聚合操作
        if (newEvents != null)
        {
            // 对新数据进行聚合操作
            for (MultiKey composed : newEvents)
            {
                IEvent[] events = (IEvent[])composed.getKeys();
                aggregator.processEnter(events, null);
            }
        }
        if (oldEvents != null)
        {
            // 对旧数据进行聚合操作
            for (MultiKey composed : oldEvents)
            {
                IEvent[] events = (IEvent[])composed.getKeys();
                aggregator.processLeave(events, null);
            }
        }
    }
    
}
