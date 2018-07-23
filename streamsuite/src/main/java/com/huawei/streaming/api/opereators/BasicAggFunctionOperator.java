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

/**
 * 基本的聚合算子
 * 
 */
public class BasicAggFunctionOperator extends InnerFunctionOperator
{
    /**
     * 聚合类的过滤条件
     * 这里都是udaf函数
     * 过滤一定发生在数据聚合之后
     * 
     * 这里的表达式一定使用的是outputSchema中的列名称
     */
    private String filterAfterAggregate;
    
    /**
     * groupby的表达式
     */
    private String groupbyExpression;
    
    /**
     * 排序
     * 允许有多个字段，之间按照逗号分割
     * 允许出现udf和udaf函数
     */
    private String orderBy;
    
    /**
     * 窗口的输出限制
     */
    private Integer limit;
    
    /**
     * <默认构造函数>
     */
    public BasicAggFunctionOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public String getFilterAfterAggregate()
    {
        return filterAfterAggregate;
    }
    
    public void setFilterAfterAggregate(String filterAfterAggregate)
    {
        this.filterAfterAggregate = filterAfterAggregate;
    }
    
    public String getGroupbyExpression()
    {
        return groupbyExpression;
    }
    
    public void setGroupbyExpression(String groupbyExpression)
    {
        this.groupbyExpression = groupbyExpression;
    }
    
    public String getOrderBy()
    {
        return orderBy;
    }
    
    public void setOrderBy(String orderBy)
    {
        this.orderBy = orderBy;
    }
    
    public Integer getLimit()
    {
        return limit;
    }
    
    public void setLimit(Integer limit)
    {
        this.limit = limit;
    }
}
