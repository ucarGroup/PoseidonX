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

package com.huawei.streaming.event;

import java.io.Serializable;

/**
 * 
 * 事件类型接口
 * 
 */
public interface IEventType extends Serializable
{
    /**
     * 获取某个属性的类型
     */
    public Attribute getAttribute(String attName);
    
    /**
     * 获得所有属性类型
     * <功能详细描述>
     */
    public Attribute[] getAllAttributes();
    
    /**
     * 获取属性名称类表
     */
    public String[] getAllAttributeNames();
    
    /**
     * 获取属性类型
     */
    public Class< ? >[] getAllAttributeTypes();
    
    /**
     * 事件类型名称
     */
    public String getEventTypeName();
    
    /**
     * 
     * 获取属性名称的编号
     */
    public int getAttributeIndex(String propertyName);
    
    /**
     * 获取指定编号的属性名称
     */
    public String getAttributeName(int index);
    
    /**
     * 获得事件类型中属性个数
     * <功能详细描述>
     */
    public int getSize();
    
}
