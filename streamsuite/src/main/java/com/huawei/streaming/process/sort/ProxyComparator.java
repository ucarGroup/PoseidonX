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

import com.huawei.streaming.common.MultiKey;

/**
 * <多值比较代理类>
 * 
 */
public class ProxyComparator implements Comparator<Object>, Serializable
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -444751756877854804L;
    
    /**
     * 多值比较类对象
     */
    private final MultiKeyComparator comparator;
    
    /**
     * <默认构造函数>
     *@param comparator 多值比较器
     */
    public ProxyComparator(MultiKeyComparator comparator)
    {
        this.comparator = comparator;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int compare(Object o1, Object o2)
    {
        return comparator.compare((MultiKey)o1, (MultiKey)o2);
    }
    
}
