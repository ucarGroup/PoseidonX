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

package com.huawei.streaming.expression;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.process.agg.aggregator.IAggregate;

/**
 * <select子句中的聚合操作表达式>
 * 
 */
public class AggregateExpression implements IExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -6767614511431633935L;
    
    private IAggregate aggegator;
    
    private boolean isDistinct;
    
    /**
     * 聚合表达式的参数
     * 这个参数，仅仅在执行器解析阶段使用
     */
    private IExpression aggArgExpression;
    
    /**
     * 聚合表达式中中的filter表达式
     * sum(a,udf(b)>100)
     */
    private IExpression aggArgFilterExpression;
    
    /**
     * <默认构造函数>
     *@param aggegator 聚合算子
     *@param isDistinct 是否去重
     */
    public AggregateExpression(IAggregate aggegator, boolean isDistinct)
    {
        this.aggegator = aggegator;
        this.isDistinct = isDistinct;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        return aggegator.getValue();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        //TODO：后续考虑是否支持
        return aggegator.getValue();
    }
    
    /**
     * <是否去重>
     */
    public boolean isDistinct()
    {
        return this.isDistinct;
    }
    
    @Override
    public Class< ? > getType()
    {
        return aggegator.getValueType();
    }
    
    public IExpression getAggArgExpression()
    {
        return aggArgExpression;
    }
    
    public void setAggArgExpression(IExpression aggArgExpression)
    {
        this.aggArgExpression = aggArgExpression;
    }
    
    public IAggregate getAggegator()
    {
        return aggegator;
    }
    
    public IExpression getAggArgFilterExpression()
    {
        return aggArgFilterExpression;
    }
    
    public void setAggArgFilterExpression(IExpression aggArgFilterExpression)
    {
        this.aggArgFilterExpression = aggArgFilterExpression;
    }
}
