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

package com.huawei.streaming.udfs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 通用的UDF函数的接口，
 * 每个函数都必须实现evaluate这个方法
 * 这个方法的入参个数不定，但是类型一定是系统支持的类型
 * 返回值也必须是系统支持的类型
 * 
 */
public abstract class UDF implements Serializable
{
    /**
     * UDF函数执行异常日志打印消息
     */
    public static final String EVALUATE_IGNORE_MESSAGE = "Ignore UDF evaluate error.";

    /**
     * 注释内容
     */
    private static final long serialVersionUID = 7734245567587838859L;
    
    private Map<String, String> config = new HashMap<String, String>();
    
    /**
     * <默认构造函数>
     */
    public UDF(Map<String, String> conf)
    {
        if (conf != null)
        {
            this.config.putAll(conf);
        }
    }
    
    public Map<String, String> getConfig()
    {
        return config;
    }
    
    public void setConfig(Map<String, String> config)
    {
        this.config = config;
    }
}
