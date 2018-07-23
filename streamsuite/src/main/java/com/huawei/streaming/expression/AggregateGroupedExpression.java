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
import com.huawei.streaming.process.agg.compute.IAggregationService;

/**
 * <具有GroupBy属性的Select子句中聚合操作表达式>
 * 
 */
public class AggregateGroupedExpression implements IExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -570897674787579681L;
    
    private IAggregationService aggegator;
    
    private int index;
    
    /**
     * <默认构造函数>
     *@param aggegator 聚合操作
     *@param index 索引
     */
    public AggregateGroupedExpression(IAggregationService aggegator, int index)
    {
        this.aggegator = aggegator;
        this.index = index;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        return aggegator.getValue(index);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        return aggegator.getValue(index);
    }
    
    @Override
    public Class< ? > getType()
    {
        return aggegator.getValueType(index);
    }
}
