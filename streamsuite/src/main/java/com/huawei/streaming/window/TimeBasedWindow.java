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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.lock.ILock;
import com.huawei.streaming.lock.LockImpl;
import com.huawei.streaming.timeservice.ITimerCallBack;
import com.huawei.streaming.timeservice.TimeService;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.ViewImpl;

/**
 * <时间窗口抽象类>
 * 
 */
public abstract class TimeBasedWindow extends ViewImpl implements IWindow, ITimerCallBack, ILock
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -1920607211633345740L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimeBasedWindow.class);
    
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
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;
    
    /**
     * <默认构造函数>
     *@param winKeepTime 窗口保持时间
     */
    public TimeBasedWindow(long winKeepTime)
    {
        super();
        if (winKeepTime > 0)
        {
            this.keepTime = winKeepTime;
            LOG.debug("Time window KeepTime: {}.", winKeepTime);
        }
        else
        {
            LOG.error("Invalid keepTime: {}.", winKeepTime);
            throw new IllegalArgumentException("Invalid keepTime: " + winKeepTime);
        }
    }
    
    /**
     * <获取时间窗口保持时间>
     */
    protected long getKeepTime()
    {
        return keepTime;
    }
    
    /**
     * <获取定时器服务>
     */
    public TimeService getTimeservice()
    {
        return timeservice;
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
        try {
            windowLock.lock();
        }catch(Exception e){
            LOG.error("##### lock error",e);
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void unlock()
    {
        try {
            windowLock.unlock();
        }catch(Exception e){
            LOG.error("##### unlock error",e);
        }
        
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
    
    /**
     * {@inheritDoc}
     */
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
    public IDataCollection getDataCollection()
    {
        return dataCollection;
    }
}
