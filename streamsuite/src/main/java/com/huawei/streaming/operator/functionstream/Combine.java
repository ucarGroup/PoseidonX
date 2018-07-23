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

package com.huawei.streaming.operator.functionstream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;

/**
 * 会合服务，负责不同流中的数据合并为一条
 */
public class Combine extends Union
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 6323780112966073904L;
    
    /**
     * 输入流列表
     */
    private List<String> inputStreamNameList;
    
    /**
     * 数据缓存
     */
    private Map<String, List<IEvent>> tempResult = new HashMap<String, List<IEvent>>();
    
    /**
     * Map<输入流名称，合并时使用的列名>
     */
    private Map<String, String> keyMap;
    
    /**
     * <默认构造函数>
     */
    public Combine(String outStreamName, IEventType outSchema, Map<String, IExpression[]> outSelect,
        List<String> inputStreamNameList, Map<String, String> keyMap)
        throws StreamingException
    {
        super(outStreamName, outSchema, outSelect);
        this.inputStreamNameList = inputStreamNameList;
        this.keyMap = keyMap;
    }
    
    /**
     * 计算事件会合结果
     */
    @Override
    public IEvent unionEvent(IEvent event)
    {
        //向缓存添加事件
        if (null == tempResult.get(event.getStreamName()))
        {
            List<IEvent> eventLists = new ArrayList<IEvent>();
            eventLists.add(event);
            tempResult.put(event.getStreamName(), eventLists);
        }
        else
        {
            tempResult.get(event.getStreamName()).add(event);
        }
        
        //生成会合事件
        return generateEvent(event);
    }
    
    /**
     * 根据新到的事件生成会合事件，如果没有达到会合条件则返回null
     */
    private IEvent generateEvent(IEvent event)
    {
        //缓存的大小小于要输出的大小，则说明流中的数据还不全
        if (tempResult.size() < super.getOutSelect().size())
        {
            return null;
        }
        
        List<Object> result = new ArrayList<Object>();
        Object keyValue = event.getValue(keyMap.get(event.getStreamName()));
        
        //循环遍历输入流名称列表，拼装会合事件
        for (String streamName : inputStreamNameList)
        {
            IEvent tempEvent = getEvent(streamName, keyValue);
            if (null == tempEvent)
            {
                return null;
            }
            IExpression[] expressions = super.getOutSelect().get(streamName);
            for (int i = 0; i < expressions.length; i++)
            {
                result.add(expressions[i].evaluate(tempEvent));
            }
        }
        
        //会合成功，则删除其对应的老事件
        removeOldEvent(keyValue);
        return new TupleEvent(super.getOutStreamName(), super.getOutSchema(), result.toArray());
    }
    
    /**
     * 删除会合成功的老事件 
     */
    private void removeOldEvent(Object keyValue)
    {
        Iterator<Entry<String, List<IEvent>>> iterator = tempResult.entrySet().iterator();
        while (iterator.hasNext())
        {
            List<IEvent> eventList = iterator.next().getValue();
            for (IEvent event : eventList)
            {
                if (event.getValue(keyMap.get(event.getStreamName())).equals(keyValue))
                {
                    eventList.remove(event);
                    break;
                }
            }
        }
    }
    
    /**
     * 根据输入流名，主键的值获得对应事件
     */
    private IEvent getEvent(String streamName, Object keyValue)
    {
        List<IEvent> eventList = tempResult.get(streamName);
        
        //循环遍历该输入流所对应的事件列表，查找键值为keyValue的事件
        for (IEvent event : eventList)
        {
            if (null != event.getValue(keyMap.get(streamName))
                && keyValue.equals(event.getValue(keyMap.get(streamName))))
            {
                return event;
            }
        }
        
        //没有找到，则返回null，没有达到会合条件
        return null;
    }
}
