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
package com.huawei.streaming.api.streams;

import java.io.Serializable;

import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

/**
 * 列信息
 * 
 */
public class Column implements Serializable
{
    private static final long serialVersionUID = 247422995585101106L;

    /**
     * 列名称
     */
    @XStreamAsAttribute
    private String name;
    
    /**
     * 列别名
     */
    @XStreamAsAttribute
    @XStreamOmitField
    private String alias;
    
    /**
     * 列类型
     * 这个是class名称
     * 比如：java.lang.String
     */
    @XStreamAsAttribute
    private String type;
    
    /**
     * <默认构造函数>
     */
    public Column(String name, Class< ? > type)
    {
        this.name = name;
        if (type != null)
        {
            this.type = type.getName();
        }
    }
    
    public String getName()
    {
        return name;
    }
    
    public void setName(String name)
    {
        this.name = name;
    }
    
    public String getType()
    {
        return type;
    }
    
    public void setType(String type)
    {
        this.type = type;
    }
    
    public String getAlias()
    {
        return alias;
    }
    
    public void setAlias(String alias)
    {
        this.alias = alias;
    }
}
