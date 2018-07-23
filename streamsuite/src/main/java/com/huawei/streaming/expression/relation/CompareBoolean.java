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

package com.huawei.streaming.expression.relation;

/**
 * Boolean类型关系运算
 * 
 * 
 */
public class CompareBoolean implements ICompare
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 7809650090416072034L;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean equals(Object left, Object right)
    {
        Boolean bl = (Boolean)left;
        Boolean br = (Boolean)right;
        
        return truthValue(bl, br, bl.compareTo(br) == 0);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean notEquals(Object left, Object right)
    {
        Boolean bl = (Boolean)left;
        Boolean br = (Boolean)right;
        
        return truthValue(bl, br, bl.compareTo(br) != 0);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean lessThan(Object left, Object right)
    {
        Boolean bl = (Boolean)left;
        Boolean br = (Boolean)right;
        
        return truthValue(bl, br, bl.compareTo(false) == 0 && br.compareTo(true) == 0);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean greaterThan(Object left, Object right)
    {
        Boolean bl = (Boolean)left;
        Boolean br = (Boolean)right;
        
        return truthValue(bl, br, bl.compareTo(true) == 0 && br.compareTo(false) == 0);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean lessOrEquals(Object left, Object right)
    {
        Boolean bl = (Boolean)left;
        Boolean br = (Boolean)right;
        
        return truthValue(bl, br, bl.compareTo(false) == 0 || br.compareTo(true) == 0);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean greaterOrEquals(Object left, Object right)
    {
        Boolean bl = (Boolean)left;
        Boolean br = (Boolean)right;
        
        return truthValue(bl, br, bl.compareTo(true) == 0 || br.compareTo(false) == 0);
    }
    
    /**
     * 判断关系表达式返回值， 当左对象和右对象为空时返回空，否则根据条件变量返回。
     * 
     */
    public static Boolean truthValue(Object left, Object right, boolean condition)
    {
        /* Return null if either operand is null */
        if (left == null || right == null)
        {
            return null;
        }
        
        /* Return the appropriate Boolean for the given truth value */
        if (condition == true)
        {
            return Boolean.TRUE;
        }
        else
        {
            return Boolean.FALSE;
        }
    }
}
