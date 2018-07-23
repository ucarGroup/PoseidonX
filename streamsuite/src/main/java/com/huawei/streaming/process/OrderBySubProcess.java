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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.process.sort.SortCondition;
import com.huawei.streaming.process.sort.SortEnum;

/**
 * orderBy操作类，对记录按指定的条件进行排序
 */
public class OrderBySubProcess implements Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 1415810148649530504L;
    
    /**
     * 排序条件，支持多个属性排序，按照链表里面的顺序为优先级进行排序
     */
    private List<SortCondition> sortConditions;
    
    /**
     * 
     * <默认构造函数>
     */
    public OrderBySubProcess(List<SortCondition> sortConditions)
    {
        super();
        this.sortConditions = sortConditions;
    }
    
    /**
     * 执行排序操作，获得排好序的事件列表
     */
    public IEvent[] process(IEvent[] theEvents)
    {
        if (null == theEvents)
        {
            return null;
        }
        
        return sort(theEvents);
    }
    
    private IEvent[] sort(IEvent[] theEvents)
    {
        //排序条件为空，则不排序直接输出
        if ((null == sortConditions) || sortConditions.isEmpty())
        {
            return (IEvent[])theEvents;
        }
        List<IEvent> result = new ArrayList<IEvent>();
        Collections.addAll(result, theEvents);
        
        //目前仅支持数字类型和字符串类型的排序
        Collections.sort(result, new Comparator<IEvent>()
        {
            @Override
            public int compare(IEvent event1, IEvent event2)
            {
                for (int i = 0; i < sortConditions.size(); i++)
                {
                    Object o1 = event1.getValue(sortConditions.get(i).getAttribute());
                    Object o2 = event2.getValue(sortConditions.get(i).getAttribute());
                    
                    //数字类型的比较
                    if (o1 instanceof Number && o2 instanceof Number)
                    {
                        //对升序和降序进行区分处理
                        if (((Number)o1).doubleValue() > ((Number)o2).doubleValue())
                        {
                            return sortConditions.get(i).getSortEnum() == SortEnum.ASC ? 1 : -1;
                        }
                        else if (((Number)o1).doubleValue() < ((Number)o2).doubleValue())
                        {
                            return sortConditions.get(i).getSortEnum() == SortEnum.ASC ? -1 : 1;
                        }
                        else
                        {
                            continue;
                        }
                    }
                    //字符串类型的比较
                    else if (o1 instanceof String && o2 instanceof String)
                    {
                        if (((String)o1).compareTo((String)o2) != 0)
                        {
                            //对升序和降序进行区分处理
                            int compareResult = ((String)o1).compareTo((String)o2);
                            if (sortConditions.get(i).getSortEnum() == SortEnum.ASC)
                            {
                                return compareResult;
                            }
                            else
                            {
                                return -compareResult;
                            }
                        }
                        else
                        {
                            continue;
                        }
                    }
                    else
                    {
                        throw new RuntimeException("Data type not supported.");
                    }
                }
                return 0;
            }
            
        });
        return (IEvent[])result.toArray(theEvents);
    }
}
