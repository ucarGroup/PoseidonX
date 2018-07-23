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

package com.huawei.streaming.operator;

import java.util.List;
import java.util.Map;

import com.huawei.streaming.application.GroupInfo;
import com.huawei.streaming.event.IEventType;

/**
 * 流处理算子基本接口
 * 所有的流处理相关的算子实现，都来源于这个算子
 * 移除所有Set接口，仅仅保留get接口，以保证属性在运行时不会被改变
 * 所有的外部Storm实现，均依赖于这个接口
 * 
 */
public interface IRichOperator extends IOperator, Configurable
{
    /**
     * 获取算子id
     */
    String getOperatorId();
    
    /**
     * 获取算子并发度
     */
    int getParallelNumber();
    
    /**
     * 获取输入流名称
     */
    List<String> getInputStream();
    
    /**
     * 获取输出流名称
     */
    String getOutputStream();
    
    /**
     * 获取输入schema
     */
    Map<String, IEventType> getInputSchema();
    
    /**
     * 获取输出schema
     */
    IEventType getOutputSchema();
    
    /**
     * 获取分组信息
     */
    Map<String, GroupInfo> getGroupInfo();
}
