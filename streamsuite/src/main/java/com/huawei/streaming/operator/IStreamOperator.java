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

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamingException;

/**
 * 流算子基本接口
 *
 */
public interface IStreamOperator extends Serializable
{
    /**
     * 设置配置属性
     * 编译时接口
     * 各种配置属性的缺失，可以在该阶段快速发现
     *
     */
    void setConfig(StreamingConfig conf) throws StreamingException;

    /**
     * 获取配置属性
     * 编译时接口
     */
    StreamingConfig getConfig();

    /**
     * 运行时的初始化接口
     *
     */
    void initialize() throws StreamingException;

    /**
     * 运行时的销毁接口
     *
     */
    void destroy() throws StreamingException;
}
