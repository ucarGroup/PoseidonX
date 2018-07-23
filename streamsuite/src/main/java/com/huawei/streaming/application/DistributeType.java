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

package com.huawei.streaming.application;

/**
 * 
 * 分布式环境中事件分发方式
 * <功能详细描述>
 * 
 */
public enum DistributeType
{
    /**
     * 随机分发
     */
    SHUFFLE("shuffle"),
    /**
     * 汇聚分发，多个源分发事件到一个目标
     */
    GLOBAL("global"),
    /**
     * 按字段分发
     */
    FIELDS("fields"),
    /**
     * 本地分发
     */
    LOCALORSHUFFLE("localorshuffle"),
    /**
     * 发给所有后续节点
     */
    ALL("all"),
    /**
     * 指定分发的目标节点
     */
    DIRECT("direct"),
    /**
     * 用户自定义
     */
    CUSTOM("custom"),
    /**
     * 无，与SHUFFLE类似
     */
    NONE("none");
    
    /**
     * 描述
     */
    private String desc;
    
    /**
     * <默认构造函数>
     */
    private DistributeType(String desc)
    {
        this.desc = desc;
    }
    
    public String getDesc()
    {
        return desc;
    }
    
    public void setDesc(String desc)
    {
        this.desc = desc;
    }
    
}
