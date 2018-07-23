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

package com.huawei.streaming.process.agg.compute;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.process.agg.aggregator.IAggregate;

/**
 * <聚合操作计算抽象类>
 * <聚合操作计算抽象类中，定义聚合操作的表达式和操作对象，对每条数据进行表达式求值，并传入操作对象进行计算。>
 * 
 */
public abstract class AggsComputeBased implements IAggregationService
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -6342039868878992630L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(AggsComputeBased.class);
    
    /**
     * 聚合算子表达式数组
     */
    private List<Pair<IExpression, IExpression>> exprs;
    
    /**
     * 聚合算子对象数组
     */
    private IAggregate aggregators[];
    
    /**
     * <默认构造函数>
     *@param exprs 聚合算子表达式数组
     *@param aggregators 聚合算子对象数组
     */
    public AggsComputeBased(List<Pair<IExpression, IExpression>> exprs, IAggregate aggregators[])
    {
        if (exprs == null || aggregators == null)
        {
            String msg = "Argument Error.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        this.exprs = exprs;
        this.aggregators = aggregators;
        
        if (exprs.size() != aggregators.length)
        {
            String msg = "Expected the same number of evaluates as aggregation methods.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * <返回表达式>
     */
    protected List<Pair<IExpression, IExpression>> getExprs()
    {
        return exprs;
    }
    
    /**
     * <返回算子>
     */
    protected IAggregate[] getAggregators()
    {
        return aggregators;
    }
    
}
