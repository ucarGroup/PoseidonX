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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.MultiKey;

/**
 * <多值比较器>
 * 
 */
public class MultiKeyComparator implements Comparator<MultiKey>, Serializable
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -5537403100518666159L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(MultiKeyComparator.class);
    
    /**
     * 排序条件，支持多个属性排序，按照链表里面的顺序为优先级进行排序
     */
    private final List<SortCondition> sortConditions;
    
    /**
     * <默认构造函数>
     *@param sortConditions 比较条件数组
     */
    public MultiKeyComparator(List<SortCondition> sortConditions)
    {
        if (null == sortConditions || sortConditions.isEmpty())
        {
            String msg = "Sort conditions is illegal.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        this.sortConditions = sortConditions;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int compare(MultiKey first, MultiKey second)
    {
        if (first.size() != sortConditions.size() || second.size() != sortConditions.size())
        {
            String msg = "Illegal Key size for comparison.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        //对每个值进行比较，发现不等返回
        for (int i = 0; i < first.size(); i++)
        {
            Object valueOne = first.get(i);
            Object valueTwo = second.get(i);
            SortEnum sortEnum = sortConditions.get(i).getSortEnum();
            
            int comparisonResult = compareValues(valueOne, valueTwo, sortEnum);
            if (comparisonResult != 0)
            {
                return comparisonResult;
            }
        }
        
        //返回相等
        return 0;
    }
    
    /**
     * <比较两个对象>
     */
    @SuppressWarnings("unchecked")
    protected static int compareValues(Object valueOne, Object valueTwo, SortEnum sortEnum)
    {
        //比较对象为空时
        if (valueOne == null || valueTwo == null)
        {
            if (valueOne == null && valueTwo == null)
            {
                return 0;
            }
            if (valueOne == null)
            {
                if (sortEnum == SortEnum.ASC)
                {
                    return -1;
                }
                return 1;
            }
            
            if (sortEnum == SortEnum.ASC)
            {
                return 1;
            }
            return -1;
        }
        
        /*//数字类型的比较
        if (valueOne instanceof Number && valueTwo instanceof Number)
        {
            //对升序和降序进行区分处理
            if (((Number) valueOne).doubleValue() > ((Number) valueTwo).doubleValue())
            {
                return sortEnum == SortEnum.ASC ? 1 : -1;
            }
            else if (((Number) valueOne).doubleValue() < ((Number) valueTwo).doubleValue())
            {
                return sortEnum == SortEnum.ASC ? -1 : 1;
            }
            else
            {
                return 0;
            }
            
        }
        //字符串类型的比较
        */
        //比较对象都不为空时
        Comparable<Object> comparable1;
        if (valueOne instanceof Comparable)
        {
            comparable1 = (Comparable<Object>)valueOne;
        }
        else
        {
            throw new ClassCastException("Cannot sort objects of type " + valueOne.getClass());
        }
        
        if (sortEnum == SortEnum.ASC)
        {
            return comparable1.compareTo(valueTwo);
        }
        
        return -1 * comparable1.compareTo(valueTwo);
        
    }
}
