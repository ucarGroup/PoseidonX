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

import com.huawei.streaming.cql.executor.operatorinfocreater.AggregaterInfoCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;

/**
 * 聚合算子
 * 里面包含了window，以及window前后的一些filter操作
 * 当然还少不了count，sum之类的UDAF函数计算和UDF函数计算
 * 
 */
@OperatorInfoCreatorAnnotation(AggregaterInfoCreator.class)
public class AggregateOperator extends BasicAggFunctionOperator
{
    /**
     * 窗口短名称
     */
    private Window window;
    
    /**
     * filter的过滤条件
     * 是一个字符串形式的逻辑表达式
     * 允许有and,or以及大括号和udf函数
     * 但是绝对不允许出现udaf函数，因为这没有聚合操作
     * 过滤发生在数据进入窗口之后，聚合之前
     * 
     * 比如 (a>1 and a <100) or ( b is not null)
     * 
     * 就是where的过滤，事件进入窗口之后，聚合之前的过滤。
     */
    private String filterBeforeAggregate;
    
    /**
     * <默认构造函数>
     */
    public AggregateOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public Window getWindow()
    {
        return window;
    }
    
    public void setWindow(Window window)
    {
        this.window = window;
    }
    
    public String getFilterBeforeAggregate()
    {
        return filterBeforeAggregate;
    }
    
    public void setFilterBeforeAggregate(String filterBeforeAggregate)
    {
        this.filterBeforeAggregate = filterBeforeAggregate;
    }
}
