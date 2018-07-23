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

package com.huawei.streaming.process;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.expression.IExpression;

/**
 * <Select处理类，对结果进行选择并过滤>
 * 
 */
public class SelectSubProcess implements Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -7498154967301014657L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(SelectSubProcess.class);
    
    /**
     * 获取属性列表达式
     */
    private final IExpression[] exprs;
    
    /**
     * 获取属性列名称
     */
    private final String[] names;
    
    /**
     * 获取属性列类型
     */
    private final Class< ? >[] types;
    
    /**
     * 过滤表达式
     */
    private final IExpression having;
    
    /**
     * 事件类型
     */
    private final IEventType eventType;
    
    /**
     * 事件名称
     */
    private final String streamName;
    
    /**
     * <默认构造函数>
     *@param streamName 流名称
     *@param expressions 选择表达式
     *@param having 过滤表达式
     *@param type 输出事件类型
     */
    public SelectSubProcess(String streamName, IExpression[] expressions, IExpression having, IEventType type)
    {
        if (null == streamName || null == expressions || null == type)
        {
            String msg = "Illegal argument.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        this.streamName = streamName;
        this.eventType = type;
        this.names = type.getAllAttributeNames();
        this.exprs = expressions;
        this.types = type.getAllAttributeTypes();
        this.having = having;
        
        if (this.exprs.length != this.names.length || this.exprs.length != this.types.length)
        {
            String msg = "Illegal expression size for select.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * <根据Select中获取表达式得到结果集>
     */
    public IEvent[] process(IEvent[] theEvents)
    {
        if (theEvents == null)
        {
            return null;
        }
        
        IEvent[] result = new IEvent[theEvents.length];
        //对于每条事件，根据表达式得到结果
        for (int i = 0; i < theEvents.length; i++)
        {
            Map<String, Object> values = new HashMap<String, Object>();
            Object[] expr = new Object[exprs.length];
            
            //TODO 是否需要根据类型进行强制转换？
            
            for (int j = 0; j < exprs.length; j++)
            {
                Object temp = exprs[j].evaluate(theEvents[i]);
                expr[j] = temp;
                values.put(names[j], expr[j]);
            }
            
            result[i] = new TupleEvent(streamName, eventType, values);
        }
        
        //如果有过滤，则进行过滤返回过滤后结果集
        if (null == having)
        {
            return result;
        }
        else
        {
            List<IEvent> list = new ArrayList<IEvent>();
            for (int i = 0; i < result.length; i++)
            {
                Boolean passesHaving = (Boolean)having.evaluate(result[i]);
                if ((passesHaving == null) || (!passesHaving))
                {
                    continue;
                }
                
                list.add(result[i]);
            }
            
            if (list.size() > 0)
            {
                return list.toArray(new IEvent[list.size()]);
            }
        }
        
        return null;
    }
    
    /**
     * <根据Select中获取表达式得到结果集>
     */
    public IEvent processSingle(IEvent theEvent)
    {
        if (theEvent == null)
        {
            return null;
        }
        
        Map<String, Object> values = new HashMap<String, Object>();
        Object[] expr = new Object[exprs.length];
        
        //TODO 是否需要根据类型进行强制转换？
        
        for (int j = 0; j < exprs.length; j++)
        {
            Object temp = exprs[j].evaluate(theEvent);
            expr[j] = temp;
            values.put(names[j], expr[j]);
        }
        
        TupleEvent result = new TupleEvent(streamName, eventType, values);
        
        return processResultHaving(result);
    }
    
    /**
     * <返回属性列表达式>
     */
    public IExpression[] getExprs()
    {
        return exprs;
    }
    
    /**
     * <返回属性列名称>
     */
    public String[] getNames()
    {
        return names;
    }
    
    /**
     * <返回属性列类型>
     */
    public Class< ? >[] getTypes()
    {
        return types;
    }
    
    /**
     * <返回过滤表达式>
     */
    public IExpression getHaving()
    {
        return having;
    }
    
    /**
     * <返回事件类型>
     */
    public IEventType getEventType()
    {
        return eventType;
    }
    
    /**
     * <返回流名称>
     */
    public String getStreamName()
    {
        return streamName;
    }
    
    /**
     * 处理Join后的Select结果集
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
            Object[] eventObj = new Object[eventType.getSize()];
            IExpression exp = null;
            for (int i = 0; i < exprs.length; i++)
            {
                exp = exprs[i];
                eventObj[i] = exp.evaluate(events);
            }
            
            TupleEvent resultEvent = new TupleEvent(streamName, eventType, eventObj);
            result[eid++] = resultEvent;
        }
        
        return result;
        
    }
    
    /**
     * <根据Select中获取表达式得到JOIN结果集>
     */
    public IEvent processSingle(IEvent[] eventsPerStream)
    {
        if (eventsPerStream == null)
        {
            return null;
        }
        
        Map<String, Object> values = new HashMap<String, Object>();
        Object[] expr = new Object[exprs.length];
        
        //TODO 是否需要根据类型进行强制转换？
        
        for (int j = 0; j < exprs.length; j++)
        {
            Object temp = exprs[j].evaluate(eventsPerStream);
            expr[j] = temp;
            values.put(names[j], expr[j]);
        }
        
        TupleEvent result = new TupleEvent(streamName, eventType, values);
        
        return processResultHaving(result);
    }
    
    private IEvent processResultHaving(TupleEvent result)
    {
        if (null == having)
        {
            return result;
        }
        else
        {
            
            Boolean passesHaving = (Boolean)having.evaluate(result);
            if ((passesHaving == null) || (!passesHaving))
            {
                return null;
            }
            
            return result;
        }
    }
}
