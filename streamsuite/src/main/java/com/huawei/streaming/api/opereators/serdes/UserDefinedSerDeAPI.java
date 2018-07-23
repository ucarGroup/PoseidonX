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

package com.huawei.streaming.api.opereators.serdes;

import java.util.TreeMap;

import com.huawei.streaming.serde.StreamSerDe;

/**
 * 
 * 用户自定义的序列化和反序列化类及其参数
 * 
 */
public class UserDefinedSerDeAPI extends SerDeAPI
{
    
    /**
     * 序列化类或者反序列化类
     * 如果是反序列化类，一定要实现StreamDeserializer接口
     * 如果是序列化类，要实现StreamSerializer接口
     */
    private Class< ? extends StreamSerDe> serDeClazz;
    
    /**
     * 配置参数
     * 为了防止输出的时候配置属性遍历顺序变化导致测试结果不一致，所以改为treemap`
     */
    private TreeMap<String, String> config;
    
    public TreeMap<String, String> getConfig()
    {
        return config;
    }
    
    public void setConfig(TreeMap<String, String> config)
    {
        this.config = config;
    }
    
    public Class< ? extends StreamSerDe> getSerDeClazz()
    {
        return serDeClazz;
    }
    
    public void setSerDeClazz(Class< ? extends StreamSerDe> serDeClazz)
    {
        this.serDeClazz = serDeClazz;
    }
    
}
