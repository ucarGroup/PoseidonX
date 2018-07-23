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

package com.huawei.streaming.process.agg.aggregator.avg;

import com.huawei.streaming.process.agg.aggregator.IAggregate;

/**
 * Long类型Avg算子
 * 
 * 
 */
public class AggregateAvgLong implements IAggregate
{
    
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 1089363818331545316L;
    
    private long sum;
    
    private long nums;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void enter(Object value, boolean filter)
    {
        enter(value);
    }
    
    /**
     * 聚合函数加入值
     * 
     */
    protected void enter(Object value)
    {
        if (value == null)
        {
            return;
        }
        
        nums++;
        sum += ((Number)value).longValue();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void leave(Object value, boolean filter)
    {
        leave(value);
    }
    
    /**
     * 聚合函数删除值
     * 
     */
    protected void leave(Object value)
    {
        if (value == null)
        {
            return;
        }
        
        nums--;
        sum -= ((Number)value).longValue();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValue()
    {
        if (nums == 0)
        {
            return null;
        }
        
        return sum / nums;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Class< ? > getValueType()
    {
        return Long.class;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        sum = 0;
        nums = 0;
    }
    
}
