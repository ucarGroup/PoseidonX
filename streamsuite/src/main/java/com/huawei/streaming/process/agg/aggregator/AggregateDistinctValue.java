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

package com.huawei.streaming.process.agg.aggregator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.RefCountedSet;

/**
 * <去重聚合算子>
 * <功能详细描述>
 * 
 */
public class AggregateDistinctValue implements IAggregate, IAggregateClone
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 355956033761507722L;
    
    private static final Logger LOG = LoggerFactory.getLogger(AggregateDistinctValue.class);
    
    /**
     * 内部聚合算子
     */
    private final IAggregate inner;
    
    /**
     * 引用个数
     */
    private final RefCountedSet<Object> valueSet;
    
    /**
     * <默认构造函数>
     *@param agg 内部聚合算子
     */
    public AggregateDistinctValue(IAggregate agg)
    {
        if (agg == null)
        {
            String msg = "Inner Aggregate Operator is null.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        this.inner = agg;
        this.valueSet = new RefCountedSet<Object>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void enter(Object value, boolean filter)
    {
        if (valueSet.add(value))
        {
            inner.enter(value, filter);
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void leave(Object value, boolean filter)
    {
        if (valueSet.remove(value))
        {
            inner.leave(value, filter);
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValue()
    {
        return inner.getValue();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        valueSet.clear();
        inner.clear();
    }
    
    /**
     * {@inheritDoc}
     */
    public IAggregate cloneAggregate()
    {
        IAggregate innerclone = ((IAggregateClone)inner).cloneAggregate();
        return new AggregateDistinctValue(innerclone);
    }
    
    @Override
    public Class< ? > getValueType()
    {
        return inner.getValueType();
    }
    
    /**
     *  返回内部聚合操作
     */
    public IAggregate getInner()
    {
        return inner;
    }
    
}
