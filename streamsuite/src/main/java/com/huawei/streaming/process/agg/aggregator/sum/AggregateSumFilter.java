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
import com.huawei.streaming.process.agg.aggregator.IAggregate;

/**
 * SUM算子, 包含过滤表达式
 * 
 * 
 */
public class AggregateSumFilter extends AggregateSum
{
    
    /**
     * ID
     */
    private static final long serialVersionUID = 6352972560206632721L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(AggregateSumFilter.class);
    
    /**
     * <默认构造函数>
     *@param type 类型
     */
    public AggregateSumFilter(Class< ? > type)
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
        
        setResultType(type);
        
        if ((type == Long.class) || (type == long.class))
        {
            setConcreteSum(new AggregateSumLongFilter());
        }
        if ((type == Integer.class) || (type == int.class))
        {
            setConcreteSum(new AggregateSumIntFilter());
        }
        if ((type == Double.class) || (type == double.class))
        {
            setConcreteSum(new AggregateSumDoubleFilter());
        }
        if ((type == Float.class) || (type == float.class))
        {
            setConcreteSum(new AggregateSumFloatFilter());
        }

        if (type == BigDecimal.class)
        {
            setConcreteSum(new AggregateSumDecimalFilter());
        }
    }
    
    /**
     * 深度clone
     */
    public IAggregate cloneAggregate()
    {
        return new AggregateSumFilter(getValueType());
    }
    
}
