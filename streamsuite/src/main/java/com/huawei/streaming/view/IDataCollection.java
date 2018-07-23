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

package com.huawei.streaming.view;

import java.io.Serializable;

import com.huawei.streaming.event.IEvent;

/**
 * <窗口事件集合>
 * <窗口通过接口将新事件和旧事件发送过来，集合中增加新事件，删除旧事件。>
 * 
 */
public interface IDataCollection extends Serializable
{
    /**
     * <接受窗口传递的新事件和旧事件>
     * <接受窗口传递的新事件和旧事件， 集合中增加新事件，删除旧事件。>
     */
    public void update(IEvent[] newData, IEvent[] oldData);
    
    /**
     * <根据已有缓存集创建新缓存集>
     * <根据已有缓存集创建新缓存集>
     */
    public IDataCollection renew();
}
