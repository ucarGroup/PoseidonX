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

/**
 * 排序条件
 */
public class SortCondition implements Serializable
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -2167911654206677516L;
    
    /**
     * 待排序的列名
     */
    private String attribute;
    
    /**
     * 排序方式，升序或降序
     */
    private SortEnum sortEnum;
    
    /**
     * 
     * <默认构造函数>
     */
    public SortCondition(String attribute, SortEnum sortEnum)
    {
        super();
        this.attribute = attribute;
        this.sortEnum = sortEnum;
    }
    
    public String getAttribute()
    {
        return attribute;
    }
    
    public void setAttribute(String attribute)
    {
        this.attribute = attribute;
    }
    
    public SortEnum getSortEnum()
    {
        return sortEnum;
    }
    
    public void setSortEnum(SortEnum sortEnum)
    {
        this.sortEnum = sortEnum;
    }
    
}
