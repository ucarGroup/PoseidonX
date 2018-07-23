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

import java.util.TreeMap;

import com.huawei.streaming.cql.executor.operatorinfocreater.DataSourceInfoOperatorCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;

/**
 * 用户自定义数据源算子
 *
 */
@OperatorInfoCreatorAnnotation(DataSourceInfoOperatorCreator.class)
public class DataSourceOperator extends BaseDataSourceOperator
{
    /**
     * 数据源的配置属性
     * 为了防止输出的时候配置属性遍历顺序变化导致测试结果不一致，所以改为treemap
     */
    private TreeMap<String, String> dataSourceConfig;
    
    /**
     * 数据源所在类名称
     */
    private String dataSourceClassName;
    
    /**
     * <默认构造函数>
     *
     */
    public DataSourceOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public String getDataSourceClassName()
    {
        return dataSourceClassName;
    }
    
    public void setDataSourceClassName(String dataSourceClassName)
    {
        this.dataSourceClassName = dataSourceClassName;
    }
    
    public TreeMap<String, String> getDataSourceConfig()
    {
        return dataSourceConfig;
    }
    
    public void setDataSourceConfig(TreeMap<String, String> dataSourceConfig)
    {
        this.dataSourceConfig = dataSourceConfig;
    }
}
