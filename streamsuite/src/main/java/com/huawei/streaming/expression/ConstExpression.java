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

package com.huawei.streaming.expression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;

/**
 * 常量表达式
 */
public class ConstExpression implements IExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -1814821159571395792L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(ConstExpression.class);
    
    /**
     * 常量值
     */
    private Object constValue;
    
    private final Class< ? > type;
    
    /**
     * 
     * <默认构造函数>
     */
    public ConstExpression(Object value)
    {
        this.constValue = value;
        if (value == null)
        {
            this.type = null;
        }
        else
        {
            this.type = value.getClass();
        }
    }
    
    /**
     * <默认构造函数>
     *@param value 常量值
     *@param type 类型
     */
    public ConstExpression(Object value, Class< ? > type)
    {
        this.constValue = value;
        if (value == null)
        {
            this.type = type;
        }
        else
        {
            this.type = value.getClass();
        }
        
    }
    
    /**
     * 执行操作，获取常量值
     */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        if (null == theEvent)
        {
            String msg = "Event is null!";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        return constValue;
    }
    
    /**
     * 根据事件列表计算表达式值
     */
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        if (null == eventsPerStream || 0 == eventsPerStream.length)
        {
            LOG.error("Streams events are null.");
            throw new RuntimeException("Streams events are null.");
        }
        
        return constValue;
    }
    
    /** {@inheritDoc} */
    @Override
    public Class< ? > getType()
    {
        return type;
    }
    
    public Object getConstValue()
    {
        return constValue;
    }
}
