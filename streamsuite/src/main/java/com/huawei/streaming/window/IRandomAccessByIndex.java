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

import java.io.Serializable;

import com.huawei.streaming.event.IEvent;

/**
 * <通过索引访问窗口数据接口>
 * <通过索引访问窗口数据>
 * 
 */
public interface IRandomAccessByIndex extends Serializable
{
    /**
     * <返回索引指定的窗口中事件>
     * <返回索引指定的窗口中事件，当isNew=true,返回新事件，否则返回旧事件。>
     */
    public IEvent getEvent(int index, boolean isNew);
}
