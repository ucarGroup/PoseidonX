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

import org.apache.commons.lang.StringUtils;

/**
 * 
 * 数据流schema中的信息：属性数据类型：属性名称
 * <功能详细描述>
 * 
 */
public class Attribute implements Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 8426637664102777131L;
    
    /**
     * 属性数据类型
     */
    private Class< ? > attDataType;
    
    /**
     * 属性名称
     */
    private String attName;
    
    /**
     * <默认构造函数>:不允许有空的属性名称
     *@param dt 属性数据类型
     *@param name 属性名称
     */
    public Attribute(Class< ? > dt, String name)
    {
        if (StringUtils.isEmpty(name))
        {
            throw new RuntimeException("The attribute name is empty.");
        }
        if (null == dt)
        {
            throw new RuntimeException("The Class of attribute is null. attribute name=" + name);
        }
        this.attDataType = dt;
        this.attName = name;
    }
    
    /**
     * 获得属性数据类型
     * <功能详细描述>
     */
    public Class< ? > getAttDataType()
    {
        return attDataType;
    }
    
    /**
     * 获得属性名称
     * <功能详细描述>
     */
    public String getAttName()
    {
        return attName;
    }
    
    /**
     * 设置属性数据类型
     * <功能详细描述>
     */
    protected void setAttDataType(Class< ? > attDataType)
    {
        this.attDataType = attDataType;
    }
    
    protected void setAttName(String attName)
    {
        this.attName = attName;
    }
    
}
