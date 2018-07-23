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

package com.huawei.streaming.window.group;

import java.util.HashMap;
import java.util.LinkedList;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;

/**
 * <基于事件時間戳跳动的分组窗口>
 * <对于每个分组值都有对应的事件保存数据结构，当该分组值对应的窗口中，最旧时间和新时间的差值是否大于窗口时间间隔。
 *  如果大于窗口时间间隔，则窗口中事件作为新事件发出，并作为下次的旧事件；否则将新事件保存在窗口中。
 *  不会影响其它分组值的处理。>
 * 
 */
public class GroupEventTimeBatchWindow extends GroupEventTimeBasedWindow
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -2146649876334354815L;
    
    /**
     * 分组窗口中每个分组对应上个批次中事件集合
     */
    private final HashMap<Object, Object> lastBatchPerKey;
    
    /**
     * 分组窗口中每个分组对应事件集合
     */
    private final HashMap<Object, Object> curDataPerKey;
    
    /**
     * 分组窗口中每个分组对应事件集合中最老时间
     */
    private final HashMap<Object, Long> curOldestTimePerKey;
    
    /**
     * <默认构造函数>
     *@param groupExprs 分組表達式
     *@param keepTime   窗口保存時間
     *@param timeExpr   時間戳表達式
     */
    public GroupEventTimeBatchWindow(IExpression[] groupExprs, long keepTime, IExpression timeExpr)
    {
        super(groupExprs, keepTime, timeExpr);
        
        lastBatchPerKey = new HashMap<Object, Object>();
        curDataPerKey = new HashMap<Object, Object>();
        curOldestTimePerKey = new HashMap<Object, Long>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void processGroupedEvent(Object subViews, IDataCollection subCollection, Object groupKey, IEvent theEvent)
    {
        @SuppressWarnings("unchecked")
        LinkedList<IEvent> curData = (LinkedList<IEvent>)curDataPerKey.get(groupKey);
        Long oldestTime = curOldestTimePerKey.get(groupKey);
        
        long timestamp = getTimestamp(theEvent);
        
        IEvent[] curBatch = null;
        if (oldestTime != null && curData != null)
        {
            if (timestamp - oldestTime >= getKeepTime())
            {
                curBatch = curData.toArray(new IEvent[curData.size()]);
                
                curData.clear();
                oldestTime = null;
            }
        }
        
        if (curData == null)
        {
            curData = new LinkedList<IEvent>();
            curDataPerKey.put(groupKey, curData);
        }
        curData.add(theEvent);
        
        if (oldestTime == null)
        {
            oldestTime = timestamp;
        }
        curOldestTimePerKey.put(groupKey, oldestTime);
        
        if (curBatch != null)
        {
            IEvent[] lastBatch = (IEvent[])lastBatchPerKey.get(groupKey);
            
            if (subCollection != null)
            {
                subCollection.update(curBatch, lastBatch);
            }
            
            if (null != subViews)
            {
                if (subViews instanceof IView)
                {
                    ((IView)subViews).update(curBatch, lastBatch);
                }
            }
            
            lastBatch = curBatch;
            lastBatchPerKey.put(groupKey, lastBatch);
        }
        
    }
    
}
