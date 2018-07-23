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

import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * 流输入算子基本接口
 *
 */
public interface IInputStreamOperator extends IStreamOperator
{
    /**
     * 运行时的执行接口
     *
     */
    void execute() throws StreamingException;

    /**
     * 设置数据发送对象
     * 运行时调用
     *
     */
    void setEmitter(IEmitter emitter);

    /**
     * 设置序列化和反序列化类
     */
    void setSerDe(StreamSerDe serde);

    /**
     * 获取序列化和反序列化类
     */
    StreamSerDe getSerDe();
}
