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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.timeservice.TimeService;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;

/**
 * <基于时间跳动的分组窗口>
 * <对每个分组值对应的窗口进行处理>
 * 
 */
public class GroupTimeBatchWindow extends GroupTimeBasedWindow
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -50603748618666373L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(GroupTimeBatchWindow.class);
    
    /**
     * 分组窗口中每个分组对应上个批次中事件 集合
     */
    private final HashMap<Object, Object> lastBatchPerKey = new HashMap<Object, Object>();
    
    /**
     * 分组窗口中每个分组对应当前批次中事件 集合
     */
    private final HashMap<Object, Object> curBatchPerKey = new HashMap<Object, Object>();
    
    /**
     * 上个批次中事件
     */
    private ArrayDeque<IEvent> lastBatch = null;
    
    /**
     * 当前批次中事件
     */
    private ArrayDeque<IEvent> curBatch = null;
    
    /**
     * <默认构造函数>
     *@param exprs 分组表达式
     *@param keepTime 窗口保持时间
     */
    public GroupTimeBatchWindow(IExpression[] exprs, long keepTime)
    {
        super(exprs, keepTime);
        
        TimeService timeservice = new TimeService(keepTime, this);
        setTimeservice(timeservice);
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void processGroupedEvent(long currentTime)
    {
        for (Entry<Object, Object> entry : getSubViewsPerKey().entrySet())
        {
            Object groupKey = entry.getKey();
            Object subViews = entry.getValue();
            IDataCollection subCollection = getSubCollectionPerKey().get(groupKey);
            
            if (null != subViews)
            {
                IEvent[] newData = null;
                IEvent[] oldData = null;
                curBatch = (ArrayDeque<IEvent>)curBatchPerKey.get(groupKey);
                if ((curBatch != null) && (!curBatch.isEmpty()))
                {
                    newData = curBatch.toArray(new IEvent[curBatch.size()]);
                }
                
                lastBatch = (ArrayDeque<IEvent>)lastBatchPerKey.get(groupKey);
                if ((lastBatch != null) && (!lastBatch.isEmpty()))
                {
                    oldData = lastBatch.toArray(new IEvent[lastBatch.size()]);
                }
                
                if ((newData != null) || (oldData != null))
                {
                    if (subCollection != null)
                    {
                        subCollection.update(null, oldData);
                    }
                    
                    if (subViews instanceof IView)
                    {
                        ((IView)subViews).update(newData, oldData);
                        
                        LOG.debug("Send Batch Window Events For GroupID: {}.", groupKey);
                    }
                }
            }
            
            lastBatch = curBatch;
            curBatchPerKey.remove(groupKey);
            lastBatchPerKey.put(groupKey, lastBatch);
            curBatch = null;
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void processGroupedEvent(Object subViews, IDataCollection subCollection, Object groupKey, IEvent theEvent)
    {
        curBatch = (ArrayDeque<IEvent>)curBatchPerKey.get(groupKey);
        
        if (null == curBatch)
        {
            curBatch = new ArrayDeque<IEvent>();
        }
        
        curBatch.add(theEvent);
        curBatchPerKey.put(groupKey, curBatch);
        curBatch = null;
    }
}
