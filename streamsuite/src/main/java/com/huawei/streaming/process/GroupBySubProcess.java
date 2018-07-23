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

package com.huawei.streaming.process;

import java.io.Serializable;

import com.huawei.streaming.expression.IExpression;

/**
 * <分组处理类>
 * 
 */
public class GroupBySubProcess implements Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 635108890449820840L;
    
    /**
     * 分组表达式
     */
    private final IExpression[] groupKeyExprs;
    
    /**
     * <默认构造函数>
     *@param groupKeyExprs 分组表达式
     */
    public GroupBySubProcess(IExpression[] groupKeyExprs)
    {
        if (null == groupKeyExprs || groupKeyExprs.length < 1)
        {
            throw new IllegalArgumentException("GroupBy key is Illegal");
        }
        
        this.groupKeyExprs = groupKeyExprs;
    }
    
    /**
     * <获取分组表达式>
     */
    public IExpression[] getGroupKeyExprs()
    {
        return groupKeyExprs;
    }
    
}
