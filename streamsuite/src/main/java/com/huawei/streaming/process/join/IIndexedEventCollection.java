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

import java.util.Set;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.event.IEvent;

/**
 * 
 * 具有索引的事件提供可查询索引接口
 * <功能详细描述>
 * 
 */
public interface IIndexedEventCollection extends IEventCollection
{
    /**
     * 获得事件的索引值
     * <功能详细描述>
     */
    public MultiKey getIndex(IEvent event);
    
    /**
     * 根据KEY值，获得同一KEY的相关事件集合
     * <功能详细描述>
     */
    public Set<IEvent> lookup(MultiKey key);
    
    /**
     * 根据KEY值，获得同一KEY的相关事件集合如果没有匹配的，返回全部值为NULL的一个事件
     * <功能详细描述>
     */
    public Set<IEvent> lookupWithNull(MultiKey key);
}
