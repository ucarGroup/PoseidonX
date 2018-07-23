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
import com.huawei.streaming.timeservice.TimeService;
import com.huawei.streaming.view.IDataCollection;

/**
 * <时间跳动窗口实现类>
 * 
 */
public class TimeBatchWindow extends TimeBasedWindow implements IBatch
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -3916243765038851148L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimeBatchWindow.class);
    

    /**
     * 窗口中当前批次中事件 
     */
    private ArrayDeque<IEvent> curBatch = new ArrayDeque<IEvent>(); //当前Batch中事件 
    
    /**
     * <默认构造函数>
     *@param keepTime 窗口保持时间
     */
    public TimeBatchWindow(long keepTime)
    {
        super(keepTime);
        TimeService timeservice = new TimeService(keepTime, this);
        setTimeservice(timeservice);
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        //TODO 未考虑窗口叠加时，上一个窗口传递的过期数据的处理。
        
        if ((null == newData) || (newData.length == 0))
        {
            LOG.error("Input Time Batch Window newData is Null!");
            return;
        }
        
        try
        {
            lock();
            for (IEvent event : newData)
            {
                curBatch.add(event);
                //LOG.debug("The newData has been added to TimeBatchWindow,current Batch size: {}", curBatch.size());
            }
        }
        finally
        {
            if (isLocked())
            {
                unlock();
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void timerCallBack(long currentTime)
    {
        //TODO 加锁操作对性能的影响，有没有更好的方法？
        try
        {
            lock();
            sendBatchData();
        }
        finally
        {
            if (isLocked())
            {
                unlock();
            }
        }
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
            IEvent[] oldData = new IEvent[0];
            if (!curBatch.isEmpty())
            {
                newData = curBatch.toArray(new IEvent[curBatch.size()]);
                curBatch.clear();
            }

            if ((newData != null) || (oldData != null))
            {
                IDataCollection dataCollection = getDataCollection();
                if (dataCollection != null)
                {
                    dataCollection.update(newData, oldData);
                }
                
                updateChild(newData, oldData);
            }
        }
        

    }
}
