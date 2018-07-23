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

/**
 * <Pari>
 * 
 */
public class Pair<K, T> implements Serializable
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -8540036915917521869L;
    
    private K first;
    
    private T second;
    
    /**
     * <默认构造函数>
     *@param first 左值
     *@param second 右值
     */
    public Pair(K first, T second)
    {
        super();
        this.first = first;
        this.second = second;
    }
    
    /**
     * <返回左值>
     */
    public K getFirst()
    {
        return first;
    }
    
    /**
     * <设置左值>
     */
    public void setFirst(K first)
    {
        this.first = first;
    }
    
    /**
     * <返回右值>
     */
    public T getSecond()
    {
        return second;
    }
    
    /**
     * <设置右值>
     */
    public void setSecond(T second)
    {
        this.second = second;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode());
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        
        if (!(obj instanceof Pair))
        {
            return false;
        }
        
        Pair other = (Pair)obj;
        
        return (first == null ? other.first == null : first.equals(other.first))
            && (second == null ? other.second == null : second.equals(other.second));
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "Pair [" + first + ':' + second + ']';
    }
    
}
