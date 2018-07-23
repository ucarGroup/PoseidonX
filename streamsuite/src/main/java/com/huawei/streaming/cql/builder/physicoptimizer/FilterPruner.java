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

package com.huawei.streaming.cql.builder.physicoptimizer;

import java.util.List;

import com.huawei.streaming.api.opereators.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.executor.ExecutorUtils;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 
 * 移除无效的filter算子
 * 
 */
public class FilterPruner implements Optimizer
{
    private static final Logger LOG = LoggerFactory.getLogger(FilterPruner.class);
    
    private List<Operator> operators;
    
    private List<OperatorTransition> transitions;
    
    private List<String> prunedOperators = Lists.newArrayList();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Application optimize(Application app)
        throws ApplicationBuildException
    {
        operators = app.getOperators();
        transitions = app.getOpTransition();
        
        checkOperators();
        prunOperators(app);
        return app;
    }
    
    private void checkOperators()
    {
        for (Operator op : operators)
        {
            if (checkOperator(op))
            {
                prunedOperators.add(op.getId());
            }
        }
    }
    
    private boolean checkOperator(Operator op)
    {
        if (!(op instanceof FilterOperator))
        {
            return false;
        }
        
        FilterOperator fop = (FilterOperator)op;
        
        if (StringUtils.isEmpty(fop.getFilterExpression()))
        {
            return true;
        }
        
        return false;
    }
    
    private void prunOperators(Application app)
        throws ApplicationBuildException
    {
        for (String opid : prunedOperators)
        {
            prun(opid, app);
        }
    }
    
    private void prun(String operatorId, Application app)
        throws ApplicationBuildException
    {
        removeTransition(operatorId, app);
        removeOperator(operatorId);
    }
    
    private void removeTransition(String operatorId, Application app)
        throws ApplicationBuildException
    {
        /*
         * Filter必然是一个入口，一个出口
         */
        List<OperatorTransition> fromTransitions = ExecutorUtils.getTransitonsByToId(operatorId, transitions);
        List<OperatorTransition> toTransitions = ExecutorUtils.getTransitonsByFromId(operatorId, transitions);
        
        if (fromTransitions.size() != 1)
        {
            LOG.error("Filter operator only allows one input stream and one output stream.");
            ApplicationBuildException exception = new ApplicationBuildException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        if (toTransitions.size() != 1)
        {
            LOG.error("Filter operator only allows one input stream and one output stream.");
            ApplicationBuildException exception = new ApplicationBuildException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        OperatorTransition fromTransition = fromTransitions.get(0);
        OperatorTransition toTransition = toTransitions.get(0);
        
        /*
         * 上一个连线一般是shuffel类型，下一个有可能是Field分发
         * 所以，移除上一个连线，保留下一个连线。
         */
        toTransition.setFromOperatorId(fromTransition.getFromOperatorId());
        transitions.remove(fromTransition);

        /**
         * 修改from operator算子中的流名称，如果有
         */
        Operator operator = ExecutorUtils.getOperatorById(fromTransition.getFromOperatorId(), app.getOperators());
        if (operator == null)
        {
            throw new ApplicationBuildException(ErrorCode.TOP_TRANSITION_FROM, fromTransition.getFromOperatorId());
        }

        if (operator instanceof SplitterOperator)
        {
            SplitterOperator spliter = (SplitterOperator)operator;
            for (SplitterSubContext sub : spliter.getSubSplitters())
            {
                if (sub.getStreamName().equals(fromTransition.getStreamName()))
                {
                    sub.setStreamName(toTransition.getStreamName());
                }
            }
        }


    }
    
    private void removeOperator(String opid)
    {
        Operator op = ExecutorUtils.getOperatorById(opid, operators);
        if (op != null)
        {
            operators.remove(op);
        }
    }
    
}
