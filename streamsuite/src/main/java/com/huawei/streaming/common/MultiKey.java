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

package com.huawei.streaming.common;

import java.io.Serializable;
import java.util.Arrays;

/**
 * <多值类>
 * 
 */
public class MultiKey implements Serializable
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 1753414850747666008L;
    
    private static final int HASH_BASE_CODE = 31;
    
    private final Object[] keys;
    
    private final int hashValue;
    
    /**
     * <默认构造函数>
     *@param keys 值数组
     */
    public MultiKey(Object[] keys)
    {
        if (keys == null)
        {
            throw new IllegalArgumentException("The array of keys must not be null.");
        }
        
        int total = 0;
        for (int i = 0; i < keys.length; i++)
        {
            if (keys[i] != null)
            {
                total *= HASH_BASE_CODE;
                total ^= keys[i].hashCode();
            }
        }
        
        this.hashValue = total;
        this.keys = keys;
    }
    
    /**
     * <返回大小>
     */
    public final int size()
    {
        return keys.length;
    }
    
    /**
     * <返回索引对应值>
     */
    public final Object get(int index)
    {
        return keys[index];
    }
    
    /**
     * {@inheritDoc}
     */
    public final boolean equals(Object other)
    {
        if (other == this)
        {
            return true;
        }
        if (other instanceof MultiKey)
        {
            MultiKey otherKeys = (MultiKey)other;
            return Arrays.equals(keys, otherKeys.keys);
        }
        return false;
    }
    
    /**
     * <返回数据>
     */
    public Object[] getKeys()
    {
        return keys;
    }
    
    /**
     * {@inheritDoc}
     */
    public final int hashCode()
    {
        return hashValue;
    }
    
    /**
     * {@inheritDoc}
     */
    public final String toString()
    {
        return "MultiKey" + Arrays.asList(keys).toString();
    }
}
