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

import java.util.List;

import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.executor.operatorinfocreater.DataSourceInfoOperatorCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;

/**
 * 进行数据源相关计算
 *
 */
@OperatorInfoCreatorAnnotation(DataSourceInfoOperatorCreator.class)
public abstract class BaseDataSourceOperator extends BasicAggFunctionOperator
{
    /**
     * 左流名称
     */
    private String leftStreamName;
    
    /**
     * 左窗口
     */
    private Window leftWindow;
    
    /**
     * join之后的过滤条件
     */
    private String filterAfterJoinExpression;
    
    /**
     * 数据源查询时参数
     */
    private List<String> queryArguments;
    
    /**
     * 数据源的schema
     * 数据源schema只能在这里定义，不能和create input Stream放在一起。
     * 在数据源定义中，允许数据源定义一次，然后通过参数变化来重复使用
     * 所以一个数据源名称，就可能对应多个schema，
     * 然后就可能在多个select子句中，出现一数据源名称为流名称的表达式，
     * 就会造成schema无法识别的混乱，
     * 所以数据源schema只能作为私有存在
     */
    private Schema dataSourceSchema;
    
    /**
     * <默认构造函数>
     *
     */
    public BaseDataSourceOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public Window getLeftWindow()
    {
        return leftWindow;
    }
    
    public void setLeftWindow(Window leftWindow)
    {
        this.leftWindow = leftWindow;
    }
    
    public String getLeftStreamName()
    {
        return leftStreamName;
    }
    
    public void setLeftStreamName(String leftStreamName)
    {
        this.leftStreamName = leftStreamName;
    }
    
    public String getFilterAfterJoinExpression()
    {
        return filterAfterJoinExpression;
    }
    
    public void setFilterAfterJoinExpression(String filterAfterJoinExpression)
    {
        this.filterAfterJoinExpression = filterAfterJoinExpression;
    }
    
    public List<String> getQueryArguments()
    {
        return queryArguments;
    }
    
    public void setQueryArguments(List<String> queryArguments)
    {
        this.queryArguments = queryArguments;
    }
    
    public Schema getDataSourceSchema()
    {
        return dataSourceSchema;
    }
    
    public void setDataSourceSchema(Schema dataSourceSchema)
    {
        this.dataSourceSchema = dataSourceSchema;
    }
    
}
