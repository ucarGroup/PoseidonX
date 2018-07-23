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

package com.huawei.streaming.timeservice;

import java.io.Serializable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <定时器服务，单位为MILLISECONDS>
 * 
 */
public class TimeService implements Serializable
{
    private static final long serialVersionUID = -1293281839532171749L;
    
    /**
     * 日志对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimeService.class);
    
    /**
     * 休眠时间（毫秒）
     */
    private static final int SLEEP_TIME = 100;
    
    /**
     * 定时器运行间隔
     */
    private long interval;
    
    /**
     * 定时器处理线程池
     */
    private transient ScheduledThreadPoolExecutor timer;
    
    /**
     * 回调任务
     */
    private ITimerCallBack timerCallback = null;
    
    /**
     * 定时器任务
     */
    private transient TimerTask timerTask;
    
    /**
     * <默认构造函数>
     *@param time 事件间隔
     *@param timerCallback 定时任务对象
     */
    public TimeService(long time, ITimerCallBack timerCallback)
    {
        this.interval = time;
        this.timerCallback = timerCallback;
    }
    
    /**
     * <启动内部定时器>
     */
    public final void startInternalClock()
    {
        if (timer != null)
        {
            LOG.warn("Internal clock is already started, stop first before starting, operation not completed.");
            return;
        }
        
        LOG.debug("Starting internal clock daemon thread, interval = {}.", interval);
        
        if (timerCallback == null)
        {
            throw new IllegalStateException("Timer callback not set.");
        }
        
        getScheduledThreadPoolExecutorDaemonThread();
        timerTask = new TimerTask(timerCallback);
        
        ScheduledFuture< ? > future = timer.scheduleAtFixedRate(timerTask, 0, interval, TimeUnit.MILLISECONDS);
        timerTask.setFuture(future);
    }
    
    
    private static class TimerThreadFactory implements ThreadFactory
    {
        // set new thread as daemon thread and name appropriately
        public Thread newThread(Runnable r)
        {
            Thread t = new Thread(r, "com.huawei.streaming.Timer");
            t.setDaemon(true);
            return t;
        }
    }
    
    /**
     * <返回定时器线程池>
     */
    private void getScheduledThreadPoolExecutorDaemonThread()
    {
        timer = new ScheduledThreadPoolExecutor(1,new TimerThreadFactory());

        timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        timer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }
    
    /**
     * <停止内部定时器>
     */
    public final void stopInternalClock()
    {
        if (timer == null)
        {
            LOG.warn("Internal clock is already stopped, start first before stopping, operation not completed.");
            return;
        }
        
        LOG.debug("Stopping internal clock daemon thread.");
        
        timer.shutdown();
        
        try
        {
            //休眠100毫秒，得到定时器线程结束
            Thread.sleep(SLEEP_TIME);
        }
        catch (InterruptedException e)
        {
            LOG.info("Timer start wait interval interruped.");
        }
        
        timer = null;
    }
}
