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
package com.huawei.streaming.api.opereators;

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * UniDirection类型
 * 左流，右流或者不限制
 * 
 */
@XStreamAlias("UniDirectionType")
public enum UniDiRectionType
{
    
    /**
     * 左流
     */
    LEFT_STREAM("left"),
    
    /**
     * 右流
     */
    RIGHT_STREAM("right"),
    
    /**
     * 全连接
     */
    NONE_STREAM("none");
    
    /**
     * 枚举类型的描述
     */
    private String desc;
    
    private UniDiRectionType(String desc)
    {
        this.desc = desc;
    }
    
    public String getDesc()
    {
        return this.desc;
    }
    
}
