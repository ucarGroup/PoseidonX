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

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.StreamClassUtil;
import com.huawei.streaming.process.agg.aggregator.AbstractAggregate;
import com.huawei.streaming.process.agg.aggregator.IAggregate;

/**
 * Sum算子
 * 
 */
public class AggregateSum extends AbstractAggregate
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 6061946410123035757L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(AggregateSum.class);
    
    private Class< ? > resultType = null;
    
    private IAggregate concreteSum;
    
    /**
     * <默认构造函数>
     *@param type 类型
     */
    public AggregateSum(Class< ? > type)
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
        
        if ((type == Long.class) || (type == long.class))
        {
            concreteSum = new AggregateSumLong();
        }
        if ((type == Integer.class) || (type == int.class))
        {
            concreteSum = new AggregateSumInt();
        }
        if ((type == Double.class) || (type == double.class))
        {
            concreteSum = new AggregateSumDouble();
        }
        if ((type == Float.class) || (type == float.class))
        {
            concreteSum = new AggregateSumFloat();
        }

        if (type == BigDecimal.class)
        {
            concreteSum = new AggregateSumDecimal();
        }
    }
    
    /**
     * 新事件来时的处理
     */
    @Override
    public void enter(Object value, boolean filter)
    {
        concreteSum.enter(value, filter);
    }
    
    /**
     * 旧事件来时的处理
     */
    @Override
    public void leave(Object value, boolean filter)
    {
        concreteSum.leave(value, filter);
    }
    
    /** {@inheritDoc} */
    
    @Override
    public Object getValue()
    {
        return concreteSum.getValue();
    }
    
    /** {@inheritDoc} */
    @Override
    public void clear()
    {
        concreteSum.clear();
    }
    
    /**
     * 深度clone
     */
    public IAggregate cloneAggregate()
    {
        return new AggregateSum(resultType);
    }
    
    /** {@inheritDoc} */
    @Override
    public Class< ? > getValueType()
    {
        return resultType;
    }
    
    /**
     * 对resultType进行赋值
     */
    protected void setResultType(Class< ? > resultType)
    {
        this.resultType = resultType;
    }
    
    /**
     * 对concreteSum进行赋值
     */
    protected void setConcreteSum(IAggregate concreteSum)
    {
        this.concreteSum = concreteSum;
    }
    
}
