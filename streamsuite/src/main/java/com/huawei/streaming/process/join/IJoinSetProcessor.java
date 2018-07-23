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
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.output.OutputType;

/**
 * <Join结果集处理接口>
 * <Join结果集处理>
 * 
 */
public interface IJoinSetProcessor
{
    /**
     * <对执行Join后的新事件和旧事件进行计算，得到相应的结果集>
     */
    public Pair<IEvent[], IEvent[]> processJoinResult(Set<MultiKey> newEvents,
                                                      Set<MultiKey> oldEvents, OutputType type);
    
    /**
     * 设置是否为单向触发JOIN
     * <功能详细描述>
     */
    public void setUnidirection(boolean uni);
    
}
