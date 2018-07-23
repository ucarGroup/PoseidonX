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
 * <聚合操作的结果合并处理类>
 * select sum(a) from stream.lengthbatchwindow(10);
 * 
 */
public class AggResultSetMergeOnly extends AggResultSetMerge
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 9030700294513518943L;
    
    /**
     * <默认构造函数>
     *@param aggregator 聚合操作
     *@param selector   选择操作
     *@param groupby    分组操作
     *@param order      排序操作
     *@param limit      限量操作
     */
    public AggResultSetMergeOnly(IAggregationService aggregator, SelectSubProcess selector, GroupBySubProcess groupby,
        OrderBySubProcess order, LimitProcess limit)
    {
        super(aggregator, selector, groupby, order, limit);
    }
    
    /**
     * {@inheritDoc}
     */
    public Pair<IEvent[], IEvent[]> processResult(IEvent[] newData, IEvent[] oldData, OutputType type)
    {
        SelectSubProcess selector = getSelector();
        
        IEvent[] selectOldEvents = null;
        IEvent[] selectNewEvents;
        
        //仅输出聚合操作结果时，当有旧数据时可输出RStream
        if (oldData != null)
        {
            if (type != OutputType.I)
            {
                selectOldEvents = selector.process(new IEvent[] {null});
            }
        }
        
        processAggregateData(newData, oldData);
        
        //处理select 语句，完成聚合操作值获取，如果其中有过滤的话，需要过滤。
        selectNewEvents = selector.process(new IEvent[] {null});
        
        //返回排序和限量后的结果集
        return orderAndLimit(selectNewEvents, selectOldEvents, type);
    }
    
    /**
     * {@inheritDoc}
     */
    public Pair<IEvent[], IEvent[]> processJoinResult(Set<MultiKey> newEvents, Set<MultiKey> oldEvents, OutputType type)
    {
        IAggregationService aggregator = getAggregator();
        SelectSubProcess selector = getSelector();
        
        IEvent[] selectOldEvents = null;
        IEvent[] selectNewEvents;
        
        if (getUnidirection())
        {
            aggregator.clearResults();
        }
        
        //仅输出聚合操作结果时，当有旧数据时可输出RStream
        if (oldEvents != null)
        {
            if (type != OutputType.I)
            {
                selectOldEvents = selector.process(new IEvent[] {null});
            }
        }
        
        processJoinAggregateData(newEvents, oldEvents);
        
        //处理select 语句，完成聚合操作值获取，如果其中有过滤的话，需要过滤。
        selectNewEvents = selector.process(new IEvent[] {null});
        
        //返回排序和限量后的结果集
        return orderAndLimit(selectNewEvents, selectOldEvents, type);
    }
}
