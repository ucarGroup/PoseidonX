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

package com.huawei.streaming.process.agg.compute;

import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.process.agg.aggregator.IAggregate;
import com.huawei.streaming.process.agg.aggregator.IAggregateClone;

/**
 * <分组聚合操作计算类>
 * <分组聚合计算，对每个分组都完成相同的聚合操作计算，每个分组具有自己的计算结果。>
 * 
 */
public class AggsComputeGrouped extends AggsComputeBased
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 8351151763800800129L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(AggsComputeGrouped.class);
    
    /**
     * 分组与分组聚合操作对象数组映射对象
     */
    private HashMap<Object, IAggregate[]> aggregatorsPerGroup;
    
    /**
     * 当前分组聚合操作对象数组
     */
    private IAggregate[] currentAggregator;
    
    /**
     * <默认构造函数>
     *@param exprs 表达式数组
     *@param aggregators 算子数组
     */
    public AggsComputeGrouped(List<Pair<IExpression, IExpression>> exprs, IAggregate[] aggregators)
    {
        super(exprs, aggregators);
        this.aggregatorsPerGroup = new HashMap<Object, IAggregate[]>();
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processEnter(IEvent theEvent, Object groupKey)
    {
        //根据分组值获取对应的聚合操作数组
        List<Pair<IExpression, IExpression>> exprs = getExprs();
        IAggregate[] groupAggregators = setCurrentAggregatorForEnter(groupKey);
        for (int j = 0; j < exprs.size(); j++)
        {
            Object result = exprs.get(j).getFirst().evaluate(theEvent);
            boolean filter = (Boolean)exprs.get(j).getSecond().evaluate(theEvent);
            groupAggregators[j].enter(result, filter);
        }
    }
    
    private IAggregate[] setCurrentAggregatorForEnter(Object groupKey)
    {
        IAggregate[] groupAggregators = aggregatorsPerGroup.get(groupKey);
        
        //如果没有，则表明该分组值为首次进入，创建该分组值对应的聚合操作的拷贝。
        if (groupAggregators == null)
        {
            groupAggregators = cloneaggregators();
            
            aggregatorsPerGroup.put(groupKey, groupAggregators);
        }
        
        //完成新数据聚合操作计算
        currentAggregator = groupAggregators;
        return groupAggregators;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processEnter(IEvent[] theEvents, Object groupKey)
    {
        List<Pair<IExpression, IExpression>> exprs = getExprs();
        IAggregate[] groupAggregators = setCurrentAggregatorForEnter(groupKey);
        for (int j = 0; j < exprs.size(); j++)
        {
            Object result = exprs.get(j).getFirst().evaluate(theEvents);
            boolean filter = (Boolean)exprs.get(j).getSecond().evaluate(theEvents);
            groupAggregators[j].enter(result, filter);
        }
        
    }
    
    /**
     * <完成聚合操作数组对象的深层次拷贝>
     */
    private IAggregate[] cloneaggregators()
    {
        IAggregate[] aggregators = getAggregators();
        IAggregate[] groupAggregators = new IAggregate[aggregators.length];
        
        for (int i = 0; i < aggregators.length; i++)
        {
            
            groupAggregators[i] = ((IAggregateClone)aggregators[i]).cloneAggregate();
        }
        return groupAggregators;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processLeave(IEvent theEvent, Object groupKey)
    {
        IAggregate[] groupAggregators = setCurrentAggregatorForLeave(groupKey);
        List<Pair<IExpression, IExpression>> exprs = getExprs();
        
        for (int j = 0; j < exprs.size(); j++)
        {
            Object result = exprs.get(j).getFirst().evaluate(theEvent);
            boolean filter = (Boolean)exprs.get(j).getSecond().evaluate(theEvent);
            groupAggregators[j].leave(result, filter);
        }
    }
    
    private IAggregate[] setCurrentAggregatorForLeave(Object groupKey)
    {
        //根据分组值获取对应的聚合操作数组
        IAggregate[] aggregators = getAggregators();
        
        IAggregate[] groupAggregators = aggregatorsPerGroup.get(groupKey);
        
        //如果没有，则表明该分组值为首次进入，创建该分组值对应的聚合操作的拷贝。
        if (groupAggregators == null)
        {
            groupAggregators = aggregators.clone();
            aggregatorsPerGroup.put(groupKey, groupAggregators);
        }
        
        //完成过期数据聚合操作计算
        currentAggregator = groupAggregators;
        return groupAggregators;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processLeave(IEvent[] theEvents, Object groupKey)
    {
        IAggregate[] groupAggregators = setCurrentAggregatorForLeave(groupKey);
        List<Pair<IExpression, IExpression>> exprs = getExprs();
        
        for (int j = 0; j < exprs.size(); j++)
        {
            Object result = exprs.get(j).getFirst().evaluate(theEvents);
            boolean filter = (Boolean)exprs.get(j).getSecond().evaluate(theEvents);
            groupAggregators[j].leave(result, filter);
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void clearResults()
    {
        aggregatorsPerGroup.clear();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValue(int column)
    {
        return currentAggregator[column].getValue();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Class< ? > getValueType(int column)
    {
        return currentAggregator[column].getValueType();
    }
    
    /**
     * {@inheritDoc}
     */
    public void setCurrentAggregator(Object groupKey)
    {
        IAggregate[] groupAggregators = aggregatorsPerGroup.get(groupKey);
        
        if (groupAggregators == null)
        {
            String msg = "Aggregator is null.";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        currentAggregator = groupAggregators;
    }
    
    @Override
    public boolean isGrouped()
    {
        return true;
    }

    @Override
    public void setAggregatorForKey(Object groupKey)
    {
        setCurrentAggregatorForEnter(groupKey);
    }
    
}
