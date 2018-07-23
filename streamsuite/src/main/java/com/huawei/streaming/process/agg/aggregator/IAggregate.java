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

package com.huawei.streaming.process.agg.aggregator;

import java.io.Serializable;

/**
 * <Aggregate算子操作接口>
 * 
 */
public interface IAggregate extends Serializable
{
    /**
     * <表达式值加入Aggregate算子>
     */
    public void enter(Object value, boolean filter);
    
    /**
     * <表达式值离开Aggregate算子>
     */
    public void leave(Object value, boolean filter);
    
    /**
     * <返回Aggregate算子结果>
     */
    public Object getValue();
    
    /**
     * <返回Aggregate算子结果类型>
     */
    public Class< ? > getValueType();
    
    /**
     * <清空Aggregate算子结果>
     */
    public void clear();
}
