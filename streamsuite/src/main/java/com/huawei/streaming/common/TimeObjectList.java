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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.huawei.streaming.event.IEvent;

/**
 * <TimeSlideWindow保存事件内部结构，记录事件时间，方便判断哪些过期。>
 * 
 */
@SuppressWarnings("rawtypes")
public class TimeObjectList implements Iterable
{
    
    private ArrayDeque<TimeObjectPair> datas = null;
    
    /**
     * <默认构造函数>
     */
    public TimeObjectList()
    {
        this.datas = new ArrayDeque<TimeObjectPair>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<TimeObjectPair> iterator()
    {
        if (null == datas)
        {
            return null;
        }
        else
        {
            return datas.iterator();
        }
    }
    
    /**
     * <加入事件>
     */
    public void add(long timestamp, Object obj)
    {
        //为空
        if (datas.isEmpty())
        {
            TimeObjectPair pair = new TimeObjectPair(timestamp, obj);
            datas.addLast(pair);
            return;
        }
        
        /**
         * 有数据，判断队列最后的时间是否等于当前插入事件时间。
         * 如果等于则更新队列中该时间的事件容器内容，将新事件加入容器中。
         */
        TimeObjectPair last = datas.getLast();
        if (last.getTimestamp() == timestamp)
        {
            if (last.getContainer() instanceof List)
            {
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>)last.getContainer();
                list.add(obj);
            }
            else
            {
                List<Object> list = new ArrayList<Object>();
                list.add(last.getContainer());
                list.add(obj);
                last.setContainer(list);
            }
            
            return;
        }
        
        //如果不等于，则当前对为全新元素插入队列中
        TimeObjectPair pair = new TimeObjectPair(timestamp, obj);
        datas.addLast(pair);
    }
    
    /**
     * <根据时间获取过期事件>
     */
    @SuppressWarnings("unchecked")
    public IEvent[] getOldData(long currentTime)
    {
        //为空
        if (datas.isEmpty())
        {
            return null;
        }
        
        //事件最老过期时间大于当前时间
        TimeObjectPair pair = datas.getFirst();
        if (pair.getTimestamp() >= currentTime)
        {
            return null;
        }
        
        //事件过期事件小于当前时间，则加入到OldData数组中
        List<IEvent> oldData = new ArrayList<IEvent>();
        while (pair.getTimestamp() < currentTime)
        {
            if (pair.getContainer() instanceof IEvent)
            {
                oldData.add((IEvent)pair.getContainer());
            }
            if (pair.getContainer() instanceof List)
            {
                oldData.addAll((List<IEvent>)pair.getContainer());
            }
            
            datas.removeFirst();
            
            if (datas.isEmpty())
            {
                break;
            }
            
            pair = datas.getFirst();
        }
        
        if (!oldData.isEmpty())
        {
            return oldData.toArray(new IEvent[oldData.size()]);
        }
        
        return null;
    }
}
