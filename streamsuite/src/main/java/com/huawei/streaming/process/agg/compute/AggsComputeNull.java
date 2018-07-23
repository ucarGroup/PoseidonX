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

import com.huawei.streaming.event.IEvent;

/**
 * <特殊类，表示无聚合操作计算>
 * 
 */
public class AggsComputeNull implements IAggregationService
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -222106774734303703L;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processEnter(IEvent theEvent, Object optionalGroupKey)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processEnter(IEvent[] theEvents, Object optionalGroupKey)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processLeave(IEvent theEvent, Object optionalGroupKeyP)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void processLeave(IEvent[] theEvents, Object optionalGroupKeyP)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void clearResults()
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValue(int column)
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Class< ? > getValueType(int column)
    {
        return null;
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
