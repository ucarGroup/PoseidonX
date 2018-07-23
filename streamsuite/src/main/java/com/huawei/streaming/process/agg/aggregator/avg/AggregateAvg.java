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

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.StreamClassUtil;
import com.huawei.streaming.process.agg.aggregator.AbstractAggregate;
import com.huawei.streaming.process.agg.aggregator.IAggregate;

/**
 * avg算子
 */
public class AggregateAvg extends AbstractAggregate
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -7634846559781265603L;
    
    private static final Logger LOG = LoggerFactory.getLogger(AggregateAvg.class);
    
    private Class< ? > resultType = null;
    
    private IAggregate concreteAvg = null;
    
    /**
     * <默认构造函数>
     *@param type 类型
     */
    public AggregateAvg(Class< ? > type)
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
        
        if ((type == Long.class) || (type == long.class))
        {
            this.resultType = Long.class;
            concreteAvg = new AggregateAvgLong();
        }
        
        if ((type == Integer.class) || (type == int.class))
        {
            this.resultType = Long.class;
            concreteAvg = new AggregateAvgLong();
        }
        
        if ((type == Double.class) || (type == double.class))
        {
            this.resultType = Double.class;
            concreteAvg = new AggregateAvgDouble();
        }
        
        if ((type == Float.class) || (type == float.class))
        {
            this.resultType = Double.class;
            concreteAvg = new AggregateAvgDouble();
        }

        if (type == BigDecimal.class)
        {
            this.resultType = BigDecimal.class;
            concreteAvg = new AggregateAvgDecimal();
        }

    }
    
    /**
     * 新事件来时的处理
     */
    @Override
    public void enter(Object value, boolean filter)
    {
        if (null == value || value.toString().isEmpty())
        {
            LOG.debug("The value is null or empty.");
            return;
        }
        
        concreteAvg.enter(value, filter);
    }
    
    /**
     * 旧事件离开时的处理
     */
    @Override
    public void leave(Object value, boolean filter)
    {
        if (null == value || value.toString().isEmpty())
        {
            LOG.debug("The value is null or empty.");
            return;
        }
        
        concreteAvg.leave(value, filter);
    }
    
    @Override
    public Object getValue()
    {
        return concreteAvg.getValue();
    }
    
    /** {@inheritDoc} */
    
    @Override
    public void clear()
    {
        concreteAvg.clear();
    }
    
    /**
     * 深度clone
     */
    public IAggregate cloneAggregate()
    {
        return new AggregateAvg(resultType);
    }
    
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
     * 对concreteAvg进行赋值
     */
    protected void setConcreteAvg(IAggregate concreteAvg)
    {
        this.concreteAvg = concreteAvg;
    }
    
}
