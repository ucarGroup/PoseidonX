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

package com.huawei.streaming.common;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import com.huawei.streaming.event.IEvent;

/**
 * <集合处理类>
 * 
 */
public class CollectionUtil
{
    
    /**
     * <将新事件加入到排序集合中，如果排序集合中已经存在新事件对应的排序，则将新事件相同排序键值事件的最前面，后续事件移出时，保证最后移出。>
     */
    public static void addEventByKeyIntoFront(Object sortKeys, IEvent event, TreeMap<Object, Object> sortedEvents)
    {
        if (null == sortKeys || null == event || null == sortedEvents)
        {
            throw new RuntimeException();
        }
        
        Object obj = sortedEvents.get(sortKeys);
        if (obj != null)
        {
            if (obj instanceof List)
            {
                @SuppressWarnings("unchecked")
                List<IEvent> events = (List<IEvent>)obj;
                events.add(0, event);
            }
            else
            {
                IEvent theEvent = (IEvent)obj;
                List<IEvent> events = new LinkedList<IEvent>();
                events.add(event);
                events.add(theEvent);
                sortedEvents.put(sortKeys, events);
            }
        }
        else
        {
            sortedEvents.put(sortKeys, event);
        }
    }
    
    /**
     * <从事件排序集合中删除待删除事件，如果集合中有该事件，则删除。否则不删除>
     */
    public static boolean removeEventByKey(Object sortKeys, IEvent event, TreeMap<Object, Object> sortedEvents)
    {
        if (null == sortKeys || null == event || null == sortedEvents)
        {
            throw new RuntimeException();
        }
        
        Object obj = sortedEvents.get(sortKeys);
        if (null == obj)
        {
            return false;
        }
        else
        {
            if (obj instanceof List)
            {
                @SuppressWarnings("unchecked")
                List<IEvent> events = (List<IEvent>)obj;
                boolean result = events.remove(event);
                if (events.isEmpty())
                {
                    sortedEvents.remove(sortKeys);
                }
                
                return result;
            }
            else if (obj.equals(event))
            {
                sortedEvents.remove(sortKeys);
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * <将新事件加入到排序集合中，如果排序集合中已经存在新事件对应的时间，则将新事件相同排序键值事件的最后面，后续事件移出时，保证最后移出。>
     */
    public static void addEventByKeyIntoBack(Long timestamp, IEvent event, TreeMap<Object, Object> sortedEvents)
    {
        if (null == timestamp || null == event || null == sortedEvents)
        {
            throw new RuntimeException();
        }
        
        Object obj = sortedEvents.get(timestamp);
        if (obj != null)
        {
            if (obj instanceof List)
            {
                @SuppressWarnings("unchecked")
                List<IEvent> events = (List<IEvent>)obj;
                events.add(event);
            }
            else
            {
                IEvent theEvent = (IEvent)obj;
                List<IEvent> events = new LinkedList<IEvent>();
                events.add(event);
                events.add(theEvent);
                sortedEvents.put(timestamp, events);
            }
        }
        else
        {
            sortedEvents.put(timestamp, event);
        }
    }
    
}
