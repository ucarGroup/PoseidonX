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

import java.io.Serializable;
import java.util.Map;

import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;

/**
 * 流处理基础算子接口
 * 这个算子里面的接口，都是运行时接口
 *
 */
public interface IOperator extends Serializable
{
    /**
     * 运行时的初始化接口
     *
     */
    void initialize(Map<String, IEmitter> emitters)
        throws StreamingException;
    
    /**
     * 运行时的执行接口
     *
     */
    void execute(String streamName, TupleEvent event)
        throws StreamingException;
    
    /**
     * 运行时的销毁接口
     *
     */
    void destroy()
        throws StreamingException;
}
