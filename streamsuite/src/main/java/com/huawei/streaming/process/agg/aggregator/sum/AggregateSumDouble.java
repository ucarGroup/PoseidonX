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

package com.huawei.streaming.process.agg.aggregator.sum;

import com.huawei.streaming.process.agg.aggregator.IAggregate;

/**
 * 
 * Double类型Sum算子
 * 
 */
class AggregateSumDouble implements IAggregate
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 7035063834321926724L;
    
    private double sum;
    
    private long nums;
    
    @Override
    public void enter(Object value, boolean filter)
    {
        enter(value);
        
    }
    
    protected void enter(Object value)
    {
        if (value == null)
        {
            return;
        }
        
        nums++;
        sum += (Double)value;
        
    }
    
    @Override
    public void leave(Object value, boolean filter)
    {
        leave(value);
        
    }
    
    protected void leave(Object value)
    {
        if (value == null)
        {
            return;
        }
        
        nums--;
        sum -= (Double)value;
        
    }
    
    @Override
    public Object getValue()
    {
        if (nums == 0)
        {
            return null;
        }
        
        return sum;
    }
    
    @Override
    public Class< ? > getValueType()
    {
        return Double.class;
    }
    
    @Override
    public void clear()
    {
        sum = 0;
        nums = 0;
    }
    
}
