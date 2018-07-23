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

package com.huawei.streaming.cql.executor.operatorviewscreater;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionGetterStrategy;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionsWalker;
import com.huawei.streaming.expression.AggregateExpression;
import com.huawei.streaming.expression.AggregateGroupedExpression;
import com.huawei.streaming.expression.ConstExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.MethodExpression;
import com.huawei.streaming.expression.OperatorBasedExpression;
import com.huawei.streaming.process.SelectSubProcess;
import com.huawei.streaming.process.agg.aggregator.IAggregate;
import com.huawei.streaming.process.agg.compute.AggsComputeGrouped;
import com.huawei.streaming.process.agg.compute.AggsComputeNoGroup;
import com.huawei.streaming.process.agg.compute.AggsComputeNull;
import com.huawei.streaming.process.agg.compute.IAggregationService;

/**
 * 创建聚合表达式
 * 聚合表达式在select表达式和聚合服务这里都会用到
 * 在聚合服务这里，有两个参数exprs, aggregators
 * exprs是聚合函数中的参数
 * aggregators是聚合函数，参数包含数据类型
 * 
 * IaggregateService要求聚合算子必须和select表达式中的聚合算子对象一样，
 * 所以就需要遍历select表达式，一旦发现聚合表达式，就加入到聚合map中去。
 * 
 */
public class AggregateServiceViewCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(AggregateServiceViewCreator.class);
    
    private boolean isGroupby = false;
    
    /**
     *  AggregateExpression和AggregateGroupedExpression的一个映射关系
     *  AggregateExpression是在select表达式中构建的，
     *  那个里面没有isgrouby的属性，也没有aggregateService，
     *  所以只能在这里构建新的AggregateGroupedExpression，
     *  然后将select表达式中的AggregateExpression替换成新的表达式
     *  这个map就保存了两个表达式之间的映射关系。
     */
    private Map<AggregateExpression, AggregateGroupedExpression> groupedExpressionMapping =
        new HashMap<AggregateExpression, AggregateGroupedExpression>();
    
    private SelectSubProcess selectProcessor;
    
    private static class AggregateExpressionGetterStrategy implements ExpressionGetterStrategy
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isEqual(IExpression exp)
        {
            if (exp instanceof AggregateExpression)
            {
                return true;
            }
            return false;
        }
        
    }

    /**
     * 创建聚合表达式
     * 
     * 
     */
    public IAggregationService create(SelectSubProcess select, boolean isHasGrupby)
        throws ExecutorException
    {
        LOG.info("start to create aggregate service");
        this.isGroupby = isHasGrupby;
        this.selectProcessor = select;
        List<IExpression> aggExpression = new ArrayList<IExpression>();
        
        ExpressionsWalker getter = new ExpressionsWalker(new AggregateExpressionGetterStrategy());
        
        for (IExpression expression : selectProcessor.getExprs())
        {
            getter.found(expression, aggExpression);
        }
        List<Pair<IExpression, IExpression>> exprs = Lists.newArrayList();
        IAggregate[] aggregators = new IAggregate[aggExpression.size()];
        
        for (int i = 0; i < aggExpression.size(); i++)
        {
            AggregateExpression aggExp = (AggregateExpression)aggExpression.get(i);
            if (aggExp.getAggArgFilterExpression() != null)
            {
                exprs.add(new Pair<IExpression, IExpression>(aggExp.getAggArgExpression(),
                    aggExp.getAggArgFilterExpression()));
            }
            else
            {
                exprs.add(new Pair<IExpression, IExpression>(aggExp.getAggArgExpression(),
                    createDefaultFilterExpression()));
            }
            aggregators[i] = aggExp.getAggegator();
        }
        
        /*
         * 对于一些聚合算子中没有udaf函数的，使用AggsComputeNull聚合服务
         */
        if (aggregators.length == 0)
        {
            return new AggsComputeNull();
        }
        
        if (isGroupby)
        {
            IAggregationService aggService = new AggsComputeGrouped(exprs, aggregators);
            createAggregateGroupbyExpreessions(aggExpression, aggService);
            replaceAggregateExpressionInSelect();
            return aggService;
        }
        
        return new AggsComputeNoGroup(exprs, aggregators);
        
    }
    
    private IExpression createDefaultFilterExpression()
    {
        return new ConstExpression(Boolean.TRUE);
    }
    
    private void replaceAggregateExpressionInSelect()
    {
        IExpression[] exps = selectProcessor.getExprs();
        for (int i = 0; i < exps.length; i++)
        {
            exps[i] = expressionChange(exps[i]);
        }
        
    }
    
    private IExpression expressionChange(IExpression expression)
    {
        if (expression instanceof AggregateExpression)
        {
            return groupedExpressionMapping.get(expression);
        }
        
        if (expression instanceof MethodExpression)
        {
            MethodExpression me = (MethodExpression)expression;
            IExpression[] exps = me.getExpr();
            for (int i = 0; i < exps.length; i++)
            {
                exps[i] = expressionChange(exps[i]);
            }
        }
        
        if (expression instanceof OperatorBasedExpression)
        {
            OperatorBasedExpression opexp = (OperatorBasedExpression)expression;
            opexp.setLeftExpr(expressionChange(opexp.getLeftExpr()));
            opexp.setRightExpr(expressionChange(opexp.getRightExpr()));
        }
        return expression;
    }
    
    private void createAggregateGroupbyExpreessions(List<IExpression> aggExpressions, IAggregationService aggService)
    {
        for (int i = 0; i < aggExpressions.size(); i++)
        {
            AggregateExpression oldexpression = (AggregateExpression)aggExpressions.get(i);
            AggregateGroupedExpression newexpression = changeAggregateToGrouped(oldexpression, aggService, i);
            groupedExpressionMapping.put(oldexpression, newexpression);
        }
    }
    
    private AggregateGroupedExpression changeAggregateToGrouped(AggregateExpression agg,
        IAggregationService aggService, int indexInAggServiceExpressions)
    {
        return new AggregateGroupedExpression(aggService, indexInAggServiceExpressions);
    }
}
