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
package com.huawei.streaming.serde;

import java.io.Serializable;
import java.util.List;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;

/**
 * 系统的反序列化接口
 * 在从输入流中读入数据的时候，要进行反序列化，才可以被系统识别。
 * 另外，反序列化还可能被应用在节点之间的数据传输。
 *
 */
public interface StreamSerDe extends Serializable
{

    /**
     * 设置配置属性
     * 编译是运行
     */
    void setConfig(StreamingConfig conf) throws StreamingException;

    /**
     * 获取配置属性
     */
    StreamingConfig getConfig();
    
    /**
     * 设置输出的schema
     *
     */
    void setSchema(TupleEventType outputSchema);


    /**
     * 获取输出schema
     */
    TupleEventType getSchema();

    /**
     * 初始化方法
     * 序列化对象一定是只初始化一次，
     * 之后每次调用，都执行执行一次序列化。
     *
     * 初始化接口在算子初始化的时候调用，如果抛出异常，会导致worker进程挂掉
     */
    void initialize() throws StreamSerDeException;

    /**
     * 反序列化
     * 将从输入流中读取的数据解析成系统可识别数据类型
     *
     * 由于可能从一行衍生出多行数据，所以返回值类型是数组类型
     */
    List<Object[]> deSerialize(Object data)
        throws StreamSerDeException;
    
    /**
     * 序列化方法
     * 将传入的事件进行序列化，转成output可以识别的对象
     * 或者字符串，或者其他类型
     *
     */
    Object serialize(List<Object[]> events)
        throws StreamSerDeException;
}
