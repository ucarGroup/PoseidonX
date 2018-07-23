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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * <对象引用个数集合>
 * 
 */
public class RefCountedSet<K> implements Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 4752151963928337823L;
    
    private HashMap<K, Integer> refSet;
    
    private int numValues;
    
    /**
     * <默认构造函数>
     *
     */
    public RefCountedSet()
    {
        refSet = new HashMap<K, Integer>();
    }
    
    /**
     * <清空集合>
     */
    public void clear()
    {
        refSet.clear();
        numValues = 0;
    }
    
    /**
     * <增加值到集合>
     * <将值加入集合中，如果值不存在集合中，则引用个数为1；如果值已经存在集合中，则将引用个数加1.>
     */
    public boolean add(K key)
    {
        Integer value = refSet.get(key);
        if (value == null)
        {
            refSet.put(key, 1);
            numValues++;
            return true;
        }
        
        value++;
        numValues++;
        refSet.put(key, value);
        return false;
    }
    
    /**
     * <增加值的引用值到集合中>
     */
    public void add(K key, int numReferences)
    {
        Integer value = refSet.get(key);
        if (value == null)
        {
            refSet.put(key, numReferences);
            numValues += numReferences;
            return;
        }
        throw new IllegalArgumentException("Key '" + key + "' already in collection");
    }
    
    /**
     * <从集合中删除值>
     * <从集合中删除值，如果集合中值对应的引用个数为0，则从集合中删除值；如果引用个数大于1， 则将引用个数减1.>
     */
    public boolean remove(K key)
    {
        Integer value = refSet.get(key);
        if (value == null)
        {
            throw new IllegalStateException("Key '" + key + "' already not in collection");
        }
        
        if (value == 1)
        {
            refSet.remove(key);
            numValues--;
            return true;
        }
        
        value--;
        refSet.put(key, value);
        numValues--;
        return false;
    }
    
    /**
     * <从集合中删除值，不管引用个数为多少>
     * <功能详细描述>
     */
    public boolean removeAll(K key)
    {
        Integer value = refSet.remove(key);
        return value != null;
    }
    
    /**
     * <返回集合迭代器>
     */
    public Iterator<Map.Entry<K, Integer>> entryIterator()
    {
        return refSet.entrySet().iterator();
    }
    
    /**
     * <返回集合Key值迭代器>
     */
    public Iterator<K> keyIterator()
    {
        return refSet.keySet().iterator();
    }
    
    /**
     * <返回集合大小>
     */
    public int size()
    {
        return numValues;
    }
    
    /**
     * <返回引用集合>
     */
    public Map<K, Integer> getRefSet()
    {
        return refSet;
    }
    
    /**
     * <返回个数>
     */
    public int getNumValues()
    {
        return numValues;
    }
    
    /**
     * <设置个数>
     */
    public void setNumValues(int numValues)
    {
        this.numValues = numValues;
    }
}
