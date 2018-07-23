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

import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.JoinFunctionOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.SplitterOperator;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.executor.ExecutorUtils;

/**
 * 将同一个输出的不同流进行合并，修改流名称
 * 
 */
public class SameStreamCombiner implements Optimizer
{
    private List<Operator> operators;
    
    private List<OperatorTransition> transitions;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Application optimize(Application app)
        throws ApplicationBuildException
    {
        operators = app.getOperators();
        transitions = app.getOpTransition();
        
        findAndChange();
        return app;
    }
    
    private void findAndChange()
    {
        for (Operator op : operators)
        {
            if (op instanceof SplitterOperator)
            {
                continue;
            }
            List<OperatorTransition> fromTransitions = ExecutorUtils.getTransitonsByFromId(op.getId(), transitions);
            if (fromTransitions.size() > 1)
            {
                for (OperatorTransition t : fromTransitions)
                {
                    String newStreamName = fromTransitions.get(0).getStreamName();
                    String oldStreamName = t.getStreamName();
                    if (!newStreamName.equals(oldStreamName))
                    {
                        replaceJoinStreamName(t, newStreamName);
                    }
                    t.setStreamName(fromTransitions.get(0).getStreamName());
                }
            }
        }
    }
    
    private void replaceJoinStreamName(OperatorTransition t, String newStreamName)
    {
        String opId = t.getToOperatorId();
        Operator op = ExecutorUtils.getOperatorById(opId, operators);
        if (null == op)
        {
            return;
        }
        
        if (!(op instanceof JoinFunctionOperator))
        {
            return;
        }
        
        String oldStreamName = t.getStreamName();
        JoinFunctionOperator jop = (JoinFunctionOperator)op;
        if (jop.getLeftStreamName().equals(oldStreamName))
        {
            jop.setLeftStreamName(newStreamName);
        }
        
        if (jop.getRightStreamName().equals(oldStreamName))
        {
            jop.setRightStreamName(newStreamName);
        }
    }
}
