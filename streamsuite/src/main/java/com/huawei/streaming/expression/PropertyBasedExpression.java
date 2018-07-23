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

package com.huawei.streaming.expression;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * <属性表达式抽象类>
 * 
 */
public abstract class PropertyBasedExpression implements IExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -5394108577477575261L;
    
    /**
     * 日志
     */
    private static final Logger LOG = LoggerFactory.getLogger(PropertyBasedExpression.class);
    
    /**
     * 属性名称
     */
    private String propertyName = null;
    
    /**
     * 属性类型
     */
    private Class< ? > propertyType = null;
    
    /**
     * <默认构造函数>
     *@param propertyName 属性名称
     *@param type 属性类型
     */
    public PropertyBasedExpression(String propertyName, Class< ? > type)
    {
        if (StringUtils.isEmpty(propertyName))
        {
            String msg = "propertyName is null or empty!";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        this.propertyName = propertyName;
        this.propertyType = type;
    }
    
    /**
     * <返回属性名称>
     */
    public String getPropertyName()
    {
        return propertyName;
    }
    
    /**
     * 对propertyName进行赋值
     */
    public void setPropertyName(String propertyName)
    {
        this.propertyName = propertyName;
    }
    
    /**
     * 返回 propertyType
     */
    public Class< ? > getPropertyType()
    {
        return propertyType;
    }
    
    /**
     * 对propertyType进行赋值
     */
    public void setPropertyType(Class< ? > propertyType)
    {
        this.propertyType = propertyType;
    }
}
