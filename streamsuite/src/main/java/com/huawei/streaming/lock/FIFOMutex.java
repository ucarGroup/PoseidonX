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

package com.huawei.streaming.lock;

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * 
 * 先进先出的互斥锁
 * 
 */
public class FIFOMutex implements Serializable
{
    private static final long serialVersionUID = 5517911381008004130L;
    
    private final AtomicBoolean locked = new AtomicBoolean(false);
    
    private final transient Queue<Thread> waiters = new ConcurrentLinkedQueue<Thread>();
    
    /**
     * < 锁定>
     */
    public void lock()
    {
        boolean wasInterrupted = false;
        Thread current = Thread.currentThread();
        waiters.add(current);
        
        // Block while not first in queue or cannot acquire lock
        while (waiters.peek() != current || !locked.compareAndSet(false, true))
        {
            LockSupport.park(this);
            if (Thread.interrupted())
            {
                wasInterrupted = true;
            }
        }
        
        waiters.remove();
        if (wasInterrupted)
        {
            current.interrupt();
        }
    }
    
    /**
     * <取消锁定>
     */
    public void unlock()
    {
        locked.set(false);
        LockSupport.unpark(waiters.peek());
    }
    
    /**
     * <是否被锁定>
     */
    public boolean isLocked()
    {
        return locked.get();
    }
}
