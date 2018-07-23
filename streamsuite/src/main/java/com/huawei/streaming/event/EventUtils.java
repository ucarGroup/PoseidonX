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

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.expression.IExpression;

/**
 * 
 * 事件工具类
 * <功能详细描述>
 * 
 */
public class EventUtils
{
    /**
     * 根据事件及设定属性名称，获得属性值相关MultiKey
     * <功能详细描述>
     */
    public static MultiKey getMultiKey(IEvent event, String[] propertyNames)
    {
        Object[] keys = getProperties(event, propertyNames);
        return new MultiKey(keys);
    }
    
    /**
     * 根据事件及设定属性名称，获得属性值
     * <功能详细描述>
     */
    public static Object[] getProperties(IEvent event, String[] propertyNames)
    {
        Object[] properties = new Object[propertyNames.length];
        for (int i = 0; i < propertyNames.length; i++)
        {
            properties[i] = event.getValue(propertyNames[i]);
        }
        return properties;
    }
    
    /**
     * 根据事件及设定表达式，获得值相关MultiKey
     * <功能详细描述>
     */
    public static MultiKey getMultiKey(IEvent event, IExpression[] exp)
    {
        Object[] keys = getExpressionValues(event, exp);
        return new MultiKey(keys);
    }
    
    /**
     * 根据事件及设定表达式，获得值
     * <功能详细描述>
     */
    public static Object[] getExpressionValues(IEvent event, IExpression[] exp)
    {
        Object[] expvalue = new Object[exp.length];
        
        for (int i = 0; i < exp.length; i++)
        {
            expvalue[i] = exp[i].evaluate(event);
        }
        return expvalue;
    }
}
