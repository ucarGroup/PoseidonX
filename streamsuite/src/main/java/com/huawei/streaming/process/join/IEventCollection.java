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

package com.huawei.streaming.process.join;

import java.io.Serializable;

import com.huawei.streaming.event.IEvent;

/**
 * 
 * 事件容器
 * <功能详细描述>
 * 
 */
public interface IEventCollection extends Serializable
{
    /**
     * Add and remove events from collection.
     * <p>
     *     It is up to the implement to decide whether to add first and then remove,
     *     or whether to remove and then add.
     * </p>
     * <p>
     *     It is important to note that a given event can be in both the
     *     removed and the added events. This means that unique indexes probably need to remove first
     *     and then add. Most other non-unique indexes will add first and then remove
     *     since the an event can be both in the add and the remove stream.
     * </p>
     */
    void addRemove(IEvent[] newData, IEvent[] oldData);
    
    /**
     * Add events to collection.
     */
    public void add(IEvent[] events);
    
    /**
     * Remove events from collection.
     */
    public void remove(IEvent[] events);
    
    /**
     * Returns true if the index is empty, or false if not
     */
    public boolean isEmpty();
    
    /**
     * Clear out index.
     */
    public void clear();
    
    /**
     * 获得保存事件的流名称
     * <功能详细描述>
     */
    public String getStreamName();
    
    /**
     * 生成一个所有属性值为NULL的空事件
     * <功能详细描述>
     */
    public IEvent genNullEvent();
}
