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

package com.huawei.streaming.api.opereators;

import com.huawei.streaming.cql.executor.operatorinfocreater.FunctorInfoCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;

/**
 * 
 * 对输出的列名称进行计算的算子
 * 
 * 比如 select (a+b) as c from S;
 * a+b 就是在这里计算了，
 * 表达式为 c=a+b
 * 
 * 
 */
@OperatorInfoCreatorAnnotation(FunctorInfoCreator.class)
public class FunctorOperator extends InnerFunctionOperator
{
    /**
     * filter的过滤条件
     * 是一个字符串形式的逻辑表达式
     * 允许有and,or以及大括号和udf函数
     * 但是绝对不允许出现udaf函数，因为这没有聚合操作
     * 
     * 比如 (a>1 and a <100) or ( b is not null)
     */
    private String filterExpression;
    
    /**
     * <默认构造函数>
     * 
     */
    public FunctorOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public String getFilterExpression()
    {
        return filterExpression;
    }
    
    public void setFilterExpression(String filterExpression)
    {
        this.filterExpression = filterExpression;
    }
}
