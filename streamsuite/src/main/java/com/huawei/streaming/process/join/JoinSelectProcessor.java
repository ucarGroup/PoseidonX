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

package com.huawei.streaming.process.join;

import java.io.Serializable;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.expression.IExpression;

/**
 * 
 * 对JOIN结果的选择输出
 * <功能详细描述>
 * 
 */
public class JoinSelectProcessor implements Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 2549163094014726129L;
    
    private static final Logger LOG = LoggerFactory.getLogger(JoinSelectProcessor.class);
    
    /**
     * 输出表达式
     */
    private final IExpression[] exprNodes;
    
    /**
     * 输出事件类型
     */
    private final IEventType outputEventType;
    
    /**
     * 输出事件名称
     */
    private final String streamName;
    
    /**
     * <默认构造函数>
     *@param streamName 事件流名称
     *@param exprNodes 针对每个流的式
     *@param type 事件类型
     */
    public JoinSelectProcessor(String streamName, IExpression[] exprNodes, IEventType type)
    {
        LOG.debug("JoinSelectProcessor Cons: stream name={},Eventtype={}.", new Object[] {streamName, type});
        this.streamName = streamName;
        this.exprNodes = exprNodes;
        this.outputEventType = type;
    }
    
    /**
     * 过滤处理
     * <功能详细描述>
     */
    public IEvent[] process(Set<MultiKey> eventsPerStream)
    {
        if (null == eventsPerStream || 0 == eventsPerStream.size())
        {
            return null;
        }
        
        IEvent[] result = new IEvent[eventsPerStream.size()];
        int eid = 0;
        for (MultiKey composed : eventsPerStream)
        {
            IEvent[] events = (IEvent[])composed.getKeys();
            Object[] eventObj = new Object[outputEventType.getSize()];
            IExpression exp = null;
            for (int i = 0; i < exprNodes.length; i++)
            {
                exp = exprNodes[i];
                eventObj[i] = exp.evaluate(events);
            }
            
            TupleEvent resultEvent = new TupleEvent(streamName, outputEventType, eventObj);
            
            result[eid++] = resultEvent;
        }
        
        return result;
        
    }
    
    public IExpression[] getExprNodes()
    {
        return exprNodes;
    }
}
