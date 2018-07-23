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

import com.huawei.streaming.cql.executor.operatorinfocreater.CombineInfoCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;

/**
 * Combine算子，将多个流的数据合并成一个
 * 从每个流中取出一个或者多列，依据combine的条件进行组合
 * 
 */
@OperatorInfoCreatorAnnotation(CombineInfoCreator.class)
public class CombineOperator extends InnerFunctionOperator
{
    
    /**
     * combine条件
     * 流的名称必须和
     * 比如s1.id,s2.aid,s3.id
     * 这里的s1,s2,s3既可以是schema名称，也可以是流名称，还有别名
     */
    private String combineProperties;
    
    /**
     * combine的流名称，如果多个流，用逗号隔离
     * 这个流出现的顺序，决定了combine的最终输出的舒徐
     * 这里的流出现顺序，必须和select中属性的流名称出现顺序一致。
     */
    private String orderedStreams;
    
    /**
     * <默认构造函数>
     */
    public CombineOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public String getCombineProperties()
    {
        return combineProperties;
    }
    
    public void setCombineProperties(String combineProperties)
    {
        this.combineProperties = combineProperties;
    }
    
    public String getOrderedStreams()
    {
        return orderedStreams;
    }
    
    public void setOrderedStreams(String orderedStreams)
    {
        this.orderedStreams = orderedStreams;
    }
    
}
