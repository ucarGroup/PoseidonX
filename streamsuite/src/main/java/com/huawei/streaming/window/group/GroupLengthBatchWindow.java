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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;

/**
 * <基于长度跳动的分组窗口>
 * <对于每个分组值都有对应的事件保存数据结构，当该分组值对应的数据大于保存长度时，发送给子视图。不会影响其它分组值的处理>
 * 
 */
public class GroupLengthBatchWindow extends GroupLengthBasedWindow implements IGroupBatch
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 2843680261675728573L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(GroupLengthBatchWindow.class);
    
    /**
     * 分组窗口中每个分组对应上个批次中事件 集合
     */
    private final HashMap<Object, Object> lastBatchPerKey;
    
    /**
     * 分组窗口中每个分组对应当前批次中事件 集合
     */
    private final HashMap<Object, Object> curBatchPerKey;
    
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
     */
    public GroupLengthBatchWindow(IExpression[] exprs, int keepLength)
    {
        super(exprs, keepLength);
        
        lastBatchPerKey = new HashMap<Object, Object>();
        
        curBatchPerKey = new HashMap<Object, Object>();
    }
    
    /**
     * <将事件加入分组值对应的子视图链>
     * <将事件加入分组值对应的Batch队列中，如果Batch队列值大于保存长度，则将数据发送到对应的子视图中>
     */
    @SuppressWarnings("unchecked")
    protected void processGroupedEvent(Object subViews, IDataCollection subCollection, Object groupKey, IEvent theEvent)
    {
        curBatch = (ArrayDeque<IEvent>)this.curBatchPerKey.get(groupKey);
        
        if (null == curBatch)
        {
            curBatch = new ArrayDeque<IEvent>();
            curBatchPerKey.put(groupKey, curBatch);
        }
        curBatch.add(theEvent);
        
        if (curBatch.size() < getKeepLength())
        {
            return;
        }
        
        sendBatchData(subViews, subCollection, groupKey);
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void sendBatchData(Object subViews, IDataCollection subCollection, Object groupKey)
    {
        lastBatch = (ArrayDeque<IEvent>)this.lastBatchPerKey.get(groupKey);
        
        if (null != subViews)
        {
            IEvent[] newData = null;
            IEvent[] oldData = null;
            
            if (!curBatch.isEmpty())
            {
                newData = curBatch.toArray(new IEvent[curBatch.size()]);
            }
            if ((lastBatch != null) && (!lastBatch.isEmpty()))
            {
                oldData = lastBatch.toArray(new IEvent[lastBatch.size()]);
            }
            
            if ((newData != null) || (oldData != null))
            {
                if (subCollection != null)
                {
                    subCollection.update(newData, oldData);
                }
                
                if (subViews instanceof IView)
                {
                    ((IView)subViews).update(newData, oldData);
                }
                
                LOG.debug("Send Batch Window Events For GroupID: {}.", groupKey);
            }
        }
        
        if (!curBatch.isEmpty())
        {
            lastBatch = curBatch;
            lastBatchPerKey.put(groupKey, lastBatch);
            curBatchPerKey.remove(groupKey);
            
        }
        
        curBatch = null;
    }
    
}
