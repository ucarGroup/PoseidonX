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

package com.huawei.streaming.process.agg.aggregator;

import java.io.Serializable;
import java.util.TreeMap;

/**
 * 为Max/Min功能提供的sort公用类
 */
public class SortCountedSet<K> implements Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -519291419203648856L;
    
    private TreeMap<K, Integer> refSet;
    
    private long countPoints;
    
    /**
     * Constructor.
     */
    public SortCountedSet()
    {
        refSet = new TreeMap<K, Integer>();
        countPoints = 0;
    }
    
    /**
     * Clear out the collection.
     */
    public void clear()
    {
        refSet.clear();
        countPoints = 0;
    }
    
    /**
     * Add a key to the set. Add with a reference count of one if the key didn't exist in the set.
     * Increase the reference count by one if the key already exists.
     */
    public void add(K key)
    {
        Integer value = refSet.get(key);
        if (value == null)
        {
            refSet.put(key, 1);
            return;
        }
        
        value++;
        refSet.put(key, value);
        countPoints++;
    }
    
    /**
     * Add a key to the set with the given number of references.
     */
    public void add(K key, int numReferences)
    {
        Integer value = refSet.get(key);
        if (value == null)
        {
            refSet.put(key, numReferences);
            return;
        }
        throw new IllegalArgumentException("Key '" + key + "' already in collection");
    }
    
    /**
     * Remove a key from the set. Removes the key if the reference count is one.
     * Decreases the reference count by one if the reference count is more then one.
     *
     */
    public void remove(K key)
    {
        Integer value = refSet.get(key);
        if (value == null)
        {
            // This could happen if a sort operation gets a remove stream that duplicates events.
            // Generally points to an invalid combination of data windows.
            // throw new IllegalStateException("Attempting to remove key from map that wasn't added");
            return;
        }
        
        countPoints--;
        if (value == 1)
        {
            refSet.remove(key);
            return;
        }
        
        value--;
        refSet.put(key, value);
    }
    
    /**
     * Returns the largest key value, or null if the collection is empty.
     */
    public K maxValue()
    {
        if (refSet.isEmpty())
        {
            return null;
        }
        return refSet.lastKey();
    }
    
    /**
     * Returns the smallest key value, or null if the collection is empty.
     */
    public K minValue()
    {
        if (refSet.isEmpty())
        {
            return null;
        }
        return refSet.firstKey();
    }
    
    /**
     * Returns the number of data points.
     */
    public long getCountPoints()
    {
        return countPoints;
    }
    
    public TreeMap<K, Integer> getRefSet()
    {
        return refSet;
    }
    
    public void setCountPoints(long countPoints)
    {
        this.countPoints = countPoints;
    }
}
