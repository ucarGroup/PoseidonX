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

import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <定时器任务，根据定时参数调用回调对象处理方法>
 * 
 */
public class TimerTask implements Runnable
{
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimerTask.class);
    
    /**
     * 定时器回调对象
     */
    private final ITimerCallBack callback;
    
    /**
     * 定时器
     */
    @SuppressWarnings("unused")
    private ScheduledFuture< ? > future = null;
    
    /**
     * <默认构造函数>
     *@param callback 定时器回调对象
     */
    public TimerTask(ITimerCallBack callback)
    {
        this.callback = callback;
    }
    
    /**
     * {@inheritDoc}
     */
    public final void run()
    {
        try
        {
            long currentTime = System.currentTimeMillis();
            callback.timerCallBack(currentTime);
        }
        catch (Throwable t)
        {
            LOG.error("Timer thread caught unhandled exception: " + t.getMessage(), t);
        }
    }
    
    /**
     * <设置运行结果>
     */
    public void setFuture(ScheduledFuture< ? > future)
    {
        this.future = future;
    }
    
}
