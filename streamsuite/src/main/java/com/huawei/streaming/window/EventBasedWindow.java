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

package com.huawei.streaming.window;

import java.util.ArrayDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IRenew;
import com.huawei.streaming.view.IView;
import com.huawei.streaming.view.ViewImpl;

/**
 * 根据事件来划分窗口，以特定的某个事件为分界来划分窗口
 */
public class EventBasedWindow extends ViewImpl implements IBatch, IWindow, IRenew
{
    
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -6696709815769384593L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(EventBasedWindow.class);
    
    /**
     * 窗口中上个批次中事件  
     */
    private ArrayDeque<IEvent> lastBatch = null;
    
    /**
     * 窗口中当前批次中事件  
     */
    private ArrayDeque<IEvent> curBatch = new ArrayDeque<IEvent>();
    
    /**
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        if (null == newData || 0 == newData.length)
        {
            LOG.error("Input Length Batch Window newData is Null!");
            return;
        }
        
        //将事件加入有效数据中
        for (IEvent newEvent : newData)
        {
            //如果是标记事件，则将本批次的事件你发送出去
            if (newEvent.isFlagEvent())
            {
                sendBatchData();
            }
            else
            {
                curBatch.add(newEvent);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setDataCollection(IDataCollection dataCollection)
    {
        if (dataCollection == null)
        {
            LOG.error("Invalid dataCollection.");
            throw new IllegalArgumentException("Invalid dataCollection");
        }
        
        this.dataCollection = dataCollection;
    }
    
    /**
     * 返回窗口事件缓存集
     */
    public IDataCollection getIDataCollection()
    {
        return dataCollection;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBatchData()
    {
        if (this.hasViews())
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
                IDataCollection datacollection = getIDataCollection();
                if (datacollection != null)
                {
                    datacollection.update(newData, oldData);
                }
                
                updateChild(newData, oldData);
                
                LOG.debug("The batchdata has been send to childView.");
            }
        }
        
        lastBatch = curBatch;
        curBatch = new ArrayDeque<IEvent>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IView renewView()
    {
        EventBasedWindow renewWindow = new EventBasedWindow();
        
        IDataCollection datacollection = getIDataCollection();
        if (datacollection != null)
        {
            IDataCollection renewCollection = datacollection.renew();
            renewWindow.setDataCollection(renewCollection);
        }
        return renewWindow;
    }
    
}
