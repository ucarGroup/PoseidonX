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
import java.util.Set;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;

/**
 * 
 * 数据流关联
 * <功能详细描述>
 * 
 */
public interface IJoinComposer extends Serializable
{
    /**
     * 保存有效窗口数据
     * <功能详细描述>
     */
    public void maintainData(IEvent[][] newDataPerStream, IEvent[][] oldDataPerStream);
    
    /**
     * 针对输入新旧事件，进行窗口有效事件的JOIN操作
     * <功能详细描述>
     */
    public Pair<Set<MultiKey>, Set<MultiKey>> join(IEvent[][] newDataPerStream, IEvent[][] oldDataPerStream);
    
    /**
     * 获得JOIN的流数量
     * <功能详细描述>
     */
    public int getStreamsSize();
}
