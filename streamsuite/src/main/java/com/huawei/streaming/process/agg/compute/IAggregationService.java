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

package com.huawei.streaming.process.agg.compute;

import java.io.Serializable;

import com.huawei.streaming.event.IEvent;

/**
 * <聚合计算接口，定义聚合计算类对事件处理方法>
 * 
 */
public interface IAggregationService extends Serializable
{
    /**
     * <对新事件进行处理>
     */
    public void processEnter(IEvent theEvent, Object optionalGroupKey);
    
    /**
     * <对旧事件进行处理>
     */
    public void processLeave(IEvent theEvent, Object optionalGroupKeyP);
    
    /**
     * <清空聚合操作结果>
     */
    public void clearResults();
    
    /**
     * <根据聚合操作在数组中的索引，得到聚合操作结果>
     */
    public Object getValue(int column);
    
    /**
     * <根据聚合操作在数组中的索引，得到聚合操作结果类型>
     */
    public Class< ? > getValueType(int column);
    
    /**
     * <根据分组键设置当前聚合操作对象，从而获取分组键对应的聚合操作结果。>
     */
    public void setCurrentAggregator(Object groupKey);
    
    /**
     * <是否具有groupby操作>
     * <功能详细描述>
     */
    public boolean isGrouped();
    
    /**
     * <对Join新事件进行处理>
     */
    public void processEnter(IEvent[] theEvents, Object optionalGroupKey);
    
    /**
     * <对Join旧事件进行处理>
     */
    public void processLeave(IEvent[] theEvents, Object optionalGroupKeyP);

    /**
     * <为每个分组创建对应聚合服务>
     */
    public void setAggregatorForKey(Object groupKey);
}
