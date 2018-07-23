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
 * 
 * <属性是否为NUll表达式>
 * 
 */
public class PropertyIsNullExpression extends PropertyBasedExpression implements IBooleanExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -8631931176479538592L;
    
    private static final Logger LOG = LoggerFactory.getLogger(PropertyIsNullExpression.class);
    
    private int streamId;
    
    /**
     * <默认构造函数>
     *@param id 流索引
     *@param propertyName 属性名称
     *@param type 属性类型
     */
    public PropertyIsNullExpression(int id, String propertyName, Class< ? > type)
    {
        super(propertyName, type);
        this.streamId = id;
    }
    
    /**
     * <默认构造函数>
     *@param propertyName 属性名称
     *@param type 属性类型
     */
    public PropertyIsNullExpression(String propertyName, Class< ? > type)
    {
        super(propertyName, type);
        this.streamId = 0;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean evaluate(IEvent theEvent)
    {
        if (null == theEvent)
        {
            throw new RuntimeException("theEvent is null!");
        }
        
        String propertyName = getPropertyName();
        Object propertyValue = theEvent.getValue(propertyName);
        
        return null == propertyValue;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        if (null == eventsPerStream || 0 == eventsPerStream.length)
        {
            LOG.error("Streams events are null.");
            throw new RuntimeException("Streams events are null.");
        }
        
        String propertyName = getPropertyName();
        Object propertyValue = eventsPerStream[streamId].getValue(propertyName);
        return null == propertyValue;
    }
    
    @Override
    public Class< ? > getType()
    {
        return Boolean.class;
    }
    
}
