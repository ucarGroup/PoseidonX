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

package com.huawei.streaming.process.sort;

import java.io.Serializable;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <单值比较器>
 * 
 */
public class ObjectComparator implements Comparator<Object>, Serializable
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -1619000855628457134L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(ObjectComparator.class);
    
    /**
     * 比较条件
     */
    private final SortCondition sortCondition;
    
    /**
     * <默认构造函数>
     *@param sortCondition 比较条件
     */
    public ObjectComparator(SortCondition sortCondition)
    {
        if (sortCondition == null)
        {
            String msg = "Sort condition is null";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        this.sortCondition = sortCondition;
    }
    
    /** {@inheritDoc} */
    @Override
    public int compare(Object first, Object second)
    {
        return MultiKeyComparator.compareValues(first, second, sortCondition.getSortEnum());
    }
    
}
