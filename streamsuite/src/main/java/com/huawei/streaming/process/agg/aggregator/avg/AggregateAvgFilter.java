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
import com.huawei.streaming.process.agg.aggregator.IAggregate;

/**
 * 条件AVG算子, 包含过滤表达式
 * 满足过滤表达式，才进行聚合运算。不满足过滤表达式，直接丢弃。
 * 
 */
public class AggregateAvgFilter extends AggregateAvg
{
    /**
     * ID
     */
    private static final long serialVersionUID = 143840877129150438L;
    
    private static final Logger LOG = LoggerFactory.getLogger(AggregateAvgFilter.class);
    
    /**
     * <默认构造函数>
     *@param type 类型
     */
    public AggregateAvgFilter(Class< ? > type)
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
            setResultType(Long.class);
            setConcreteAvg(new AggregateAvgLongFilter());
        }
        
        if ((type == Integer.class) || (type == int.class))
        {
            setResultType(Long.class);
            setConcreteAvg(new AggregateAvgLongFilter());
        }
        
        if ((type == Double.class) || (type == double.class))
        {
            setResultType(Double.class);
            setConcreteAvg(new AggregateAvgDoubleFilter());
        }
        
        if ((type == Float.class) || (type == float.class))
        {
            setResultType(Double.class);
            setConcreteAvg(new AggregateAvgDoubleFilter());
        }

        if (type == BigDecimal.class)
        {
            setResultType(BigDecimal.class);
            setConcreteAvg(new AggregateAvgDecimalFilter());
        }
    }
    
    /**
     * 深度clone
     */
    public IAggregate cloneAggregate()
    {
        return new AggregateAvgFilter(getValueType());
    }
}
