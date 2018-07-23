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

package com.huawei.streaming.process.agg.aggregator.count;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.process.agg.aggregator.AbstractAggregate;
import com.huawei.streaming.process.agg.aggregator.IAggregate;

/**
 * count算子
 */
public class AggregateCount extends AbstractAggregate
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -7202385631919932414L;
    
    private static final Logger LOG = LoggerFactory.getLogger(AggregateCount.class);
    
    private Class< ? > resultType = null;
    
    private long currentSize;
    
    /**
     * <默认构造函数>
     *@param type 类型
     */
    public AggregateCount(Class< ? > type)
    {
        super(type);
        resultType = Long.class;
    }
    
    /** {@inheritDoc} */
    @Override
    public void enter(Object value, boolean filter)
    {
        enter(value);
    }
    
    /**
     * 新增数据计算
     */
    protected void enter(Object value)
    {
        if (value == null)
        {
            LOG.error("The input value in enter is null.");
            return;
        }
        else
        {
            currentSize++;
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public void leave(Object value, boolean filter)
    {
        leave(value);
    }
    
    /**
     * 过期数据计算
     */
    protected void leave(Object value)
    {
        if (value == null)
        {
            LOG.error("The input value in leave is null.");
            return;
        }
        else
        {
            currentSize--;
        }
    }
    
    /** {@inheritDoc} */
    
    @Override
    public Object getValue()
    {
        return currentSize;
    }
    
    /** {@inheritDoc} */
    
    @Override
    public void clear()
    {
        currentSize = 0;
    }
    
    /**
     * 深度clone
     */
    public IAggregate cloneAggregate()
    {
        return new AggregateCount(resultType);
    }
    
    @Override
    public Class< ? > getValueType()
    {
        return Long.class;
    }
    
}
