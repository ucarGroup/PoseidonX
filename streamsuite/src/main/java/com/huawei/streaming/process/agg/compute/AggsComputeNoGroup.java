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

import java.util.List;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.process.agg.aggregator.IAggregate;

/**
 * <非分组聚合操作计算类>
 * <聚合操作计算>
 * 
 */
public class AggsComputeNoGroup extends AggsComputeBased
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -1682490879529614680L;
    
    /**
     * <默认构造函数>
     *@param exprs 聚合算子表达式数组
     *@param aggregators 聚合算子对象数组
     */
    public AggsComputeNoGroup(List<Pair<IExpression, IExpression>> exprs, IAggregate[] aggregators)
    {
        super(exprs, aggregators);
    }
    
    /**
     * {@inheritDoc}
     */
    public Object getValue(int column)
    {
        IAggregate[] aggregators = getAggregators();
        
        return aggregators[column].getValue();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Class< ? > getValueType(int column)
    {
        IAggregate[] aggregators = getAggregators();
        
        return aggregators[column].getValueType();
    }
    
    /**
     * {@inheritDoc}
     */
    public void clearResults()
    {
        IAggregate[] aggregators = getAggregators();
        
        for (IAggregate aggregator : aggregators)
        {
            aggregator.clear();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processEnter(IEvent theEvent, Object optionalGroupKey)
    {
        IAggregate[] aggregators = getAggregators();
        List<Pair<IExpression, IExpression>> exprs = getExprs();
        
        //对每个聚合操作进行计算
        for (int j = 0; j < exprs.size(); j++)
        {
            Object columnResult = exprs.get(j).getFirst().evaluate(theEvent);
            boolean filter = (Boolean)exprs.get(j).getSecond().evaluate(theEvent);
            aggregators[j].enter(columnResult, filter);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processEnter(IEvent[] theEvents, Object optionalGroupKey)
    {
        IAggregate[] aggregators = getAggregators();
        List<Pair<IExpression, IExpression>> exprs = getExprs();
        
        //对每个聚合操作进行计算
        for (int j = 0; j < exprs.size(); j++)
        {
            Object columnResult = exprs.get(j).getFirst().evaluate(theEvents);
            boolean filter = (Boolean)exprs.get(j).getSecond().evaluate(theEvents);
            aggregators[j].enter(columnResult, filter);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processLeave(IEvent theEvent, Object optionalGroupKey)
    {
        IAggregate[] aggregators = getAggregators();
        List<Pair<IExpression, IExpression>> exprs = getExprs();
        
        //对每个聚合操作进行计算
        for (int j = 0; j < exprs.size(); j++)
        {
            Object columnResult = exprs.get(j).getFirst().evaluate(theEvent);
            boolean filter = (Boolean)exprs.get(j).getSecond().evaluate(theEvent);
            aggregators[j].leave(columnResult, filter);
            
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processLeave(IEvent[] theEvents, Object optionalGroupKey)
    {
        IAggregate[] aggregators = getAggregators();
        List<Pair<IExpression, IExpression>> exprs = getExprs();
        
        //对每个聚合操作进行计算
        for (int j = 0; j < exprs.size(); j++)
        {
            Object columnResult = exprs.get(j).getFirst().evaluate(theEvents);
            boolean filter = (Boolean)exprs.get(j).getSecond().evaluate(theEvents);
            aggregators[j].leave(columnResult, filter);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setCurrentAggregator(Object groupKey)
    {
        
    }
    
    @Override
    public boolean isGrouped()
    {
        return false;
    }

    @Override
    public void setAggregatorForKey(Object groupKey)
    {
        
    }
}
