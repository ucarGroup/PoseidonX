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

package com.huawei.streaming.process.agg.aggregator.min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.StreamClassUtil;
import com.huawei.streaming.process.agg.aggregator.AbstractAggregate;
import com.huawei.streaming.process.agg.aggregator.IAggregate;
import com.huawei.streaming.process.agg.aggregator.SortCountedSet;

/**
 * Min算子
 */
public class AggregateMin extends AbstractAggregate
{
    
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 7351285672219814761L;
    
    private static final Logger LOG = LoggerFactory.getLogger(AggregateMin.class);
    
    private SortCountedSet<Object> sortSet;
    
    private Class< ? > resultType = null;
    
    /**
     * <默认构造函数>
     *@param type 类型
     */
    public AggregateMin(Class< ? > type)
    {
        super(type);
        
        if (null == type)
        {
            String msg = "Type is null.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!StreamClassUtil.isNumberic(type))
        {
            String msg = "Type is not numberic type.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        this.resultType = type;
        this.sortSet = new SortCountedSet<Object>();
    }
    
    /** {@inheritDoc} */
    @Override
    public void enter(Object value, boolean filter)
    {
        enter(value);
    }
    
    /**
     * 添加数据进行计算
     */
    protected void enter(Object value)
    {
        if (value == null)
        {
            LOG.info("Input in enter method when computing min is null.");
            return;
        }
        sortSet.add(value);
    }
    
    /** {@inheritDoc} */
    @Override
    public void leave(Object value, boolean filter)
    {
        leave(value);
    }
    
    /**
     * 数据过期计算
     */
    protected void leave(Object value)
    {
        if (value == null)
        {
            LOG.info("Input in leave method when computing min is null.");
            return;
        }
        sortSet.remove(value);
    }
    
    @Override
    public Object getValue()
    {
        return sortSet.minValue();
    }
    
    /** {@inheritDoc} */
    @Override
    public void clear()
    {
        sortSet.clear();
    }
    
    public SortCountedSet<Object> getSortSet()
    {
        return sortSet;
    }
    
    /**
     * 深度clone
     */
    public IAggregate cloneAggregate()
    {
        return new AggregateMin(resultType);
    }
    
    @Override
    public Class< ? > getValueType()
    {
        return resultType;
    }
    
}
