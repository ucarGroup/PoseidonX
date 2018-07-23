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

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * 功能性算子，主要为系统提供window，join，orderby，group by等聚合操作。
 * 
 * 这里的名称和operator包中的不一样
 * 这里定义的这些operator，主要是进行执行计划的序列化和反序列化的。
 * 所有的数据类型全部是字符串类型，
 * 之后还要经过语法的解析，物理执行计划的优化之后，才会在application中提交。
 * 
 */
@XStreamAlias("Function")
public class InnerFunctionOperator extends Operator
{
    /**
     * 输出的列定义，
     * 不光有单纯的列，还有udf以及udaf函数
     */
    private String outputExpression;
    
    /**
     * <默认构造函数>
     */
    public InnerFunctionOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public String getOutputExpression()
    {
        return outputExpression;
    }
    
    public void setOutputExpression(String outputExpression)
    {
        this.outputExpression = outputExpression;
    }
    
}
