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

package com.huawei.streaming.util.datatype;

import java.io.Serializable;

import com.huawei.streaming.exception.StreamingException;

/**
 * 数据类型解析
 * Created by h00183771 on 2015/11/18.
 * 以前使用的是枚举类型，只会有一个实例，
 * 不适合多客户端多时区场景，没办法实现CQL的多客户端并发。
 */
public interface DataTypeParser extends Serializable
{
    /**
     * 创建对应数据类型实例，如果为空，返回null
     *
     */
     Object createValue(String value) throws StreamingException;

    /**
     * 将原始数据类型转为对应的数据类型字符串形式
     */
    String toStringValue(Object value) throws StreamingException;

}
