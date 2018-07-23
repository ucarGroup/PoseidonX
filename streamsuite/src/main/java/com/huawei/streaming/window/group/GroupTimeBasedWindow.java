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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.lock.ILock;
import com.huawei.streaming.lock.LockImpl;
import com.huawei.streaming.timeservice.ITimerCallBack;
import com.huawei.streaming.timeservice.TimeService;

/**
 * <基于时间的分组窗口抽象类>
 * 
 */
public abstract class GroupTimeBasedWindow extends GroupWindowImpl implements ITimerCallBack, ILock
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 1425042512530682084L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(GroupTimeBasedWindow.class);
    
    /**
     * 窗口事件保持时间
     */
    private long keepTime;
    
    /**
     * 定时器服务
     */
    private TimeService timeservice = null;
    
    /**
     * 锁对象，当前线程与定时器线程访问数据时需要先获取锁。
     */
    private ILock windowLock;
    
    /**
     * <默认构造函数>
     *@param exprs 分组表达式
     *@param winKeepTime 窗口保持时间
     */
    public GroupTimeBasedWindow(IExpression[] exprs, long winKeepTime)
    {
        super(exprs);
        
        if (winKeepTime > 0)
        {
            this.keepTime = winKeepTime;
        }
        else
        {
            LOG.error("Invalid keepTime: {}.", winKeepTime);
            throw new IllegalArgumentException("Invalid keepTime: " + winKeepTime);
        }
    }
    
    /** {@inheritDoc} */
    
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        //TODO 未考虑窗口叠加时，上一个窗口传递的过期数据的处理。
        
        if ((null == newData) || (0 == newData.length))
        {
            LOG.error("Input Time Batch Window newData is Null!");
            return;
        }
        
        try
        {
            lock();
            super.update(newData, oldData);
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
     * <设置定时器服务>
     */
    public void setTimeservice(TimeService timeservice)
    {
        this.timeservice = timeservice;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void lock()
    {
        windowLock.lock();
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void unlock()
    {
        windowLock.unlock();
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLocked()
    {
        return windowLock.isLocked();
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
            processGroupedEvent(currentTime);
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
     * <处理分组窗口内事件>
     */
    protected abstract void processGroupedEvent(long currentTime);
    
    /**
     * <获取窗口保持时间>
     */
    protected long getKeepTime()
    {
        return keepTime;
    }
    
    /**
     * 初始化锁对象
     * 
     */
    public void initLock()
    {
        if (windowLock == null)
        {
            windowLock =  new LockImpl();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        initLock();
        
        if (timeservice != null)
        {
            timeservice.startInternalClock();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        if (timeservice != null)
        {
            timeservice.stopInternalClock();
        }
    }
}
