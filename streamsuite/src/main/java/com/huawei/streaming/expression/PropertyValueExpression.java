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
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * <属性值表达式>
 *
 */
public class PropertyValueExpression extends PropertyBasedExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -6772060263353223976L;
    
    private static final Logger LOG = LoggerFactory.getLogger(PropertyValueExpression.class);
    
    private int streamIndex;
    
    private int propertyIndex = -1;
    
    /**
     * <默认构造函数>
     *
     */
    public PropertyValueExpression(int id, String propertyName, Class< ? > type)
        throws StreamingException
    {
        super(propertyName, type);
        
        if (id < 0)
        {
            LOG.error("Schema index in property value expression must bigger than 0.");
            throw new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
        }
        
        this.streamIndex = id;
    }
    
    /**
     * <默认构造函数>
     *
     */
    public PropertyValueExpression(String propertyName, Class< ? > type)
    {
        super(propertyName, type);
        this.streamIndex = 0;
    }
    
    /**
     * 返回 streamIndex
     *
     */
    public final int getStreamIndex()
    {
        return streamIndex;
    }
    
    public void setStreamIndex(int streamIndex)
    {
        this.streamIndex = streamIndex;
    }
    
    /**
     * {@inheritDoc}
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
        
        if (propertyIndex == -1)
        {
            propertyIndex = theEvent.getIndexByPropertyName(getPropertyName());
        }
        
        return theEvent.getValue(propertyIndex);
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
        return eventsPerStream[streamIndex].getValue(propertyName);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Class< ? > getType()
    {
        return getPropertyType();
    }
    
}
