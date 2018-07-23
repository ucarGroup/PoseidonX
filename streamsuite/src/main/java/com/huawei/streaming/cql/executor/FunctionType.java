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

package com.huawei.streaming.cql.executor;

/**
 * 函数类型
 * 
 */
public enum FunctionType
{
    
    /**
     * 一般的函数
     * 一个输入，一个输出
     */
    UDF("udf"),
    
    /**
     * 聚合函数
     * 多个输入，一个输出
     */
    UDAF("udaf"),
    
    /**
     * 展开的函数
     * 一个输入，多个输出
     */
    UDTF("udtf");
    
    /**
     * 枚举类型的描述
     */
    private String desc;
    
    private FunctionType(String desc)
    {
        this.desc = desc;
    }
    
    public String getDesc()
    {
        return this.desc;
    }
    
}
