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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.cql.exception.ApplicationBuildException;

/**
 * 移除重复的连线
 * 
 */
public class SameTransitionPruner implements Optimizer
{
    private List<OperatorTransition> transitions;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Application optimize(Application app)
        throws ApplicationBuildException
    {
        transitions = app.getOpTransition();
        findAndRemove();
        return app;
    }
    
    private void findAndRemove()
    {
        List<OperatorTransition> tsNeedRemoved = new ArrayList<OperatorTransition>();
        found(tsNeedRemoved);
        remove(tsNeedRemoved);
    }
    
    private void found(List<OperatorTransition> tsNeedRemoved)
    {
        Set<String> ts = new HashSet<String>();
        for (OperatorTransition op : transitions)
        {
            String st = op.toString();
            if (ts.contains(st))
            {
                tsNeedRemoved.add(op);
            }
            else
            {
                ts.add(op.toString());
            }
        }
    }
    
    private void remove(List<OperatorTransition> tsNeedRemoved)
    {
        for (OperatorTransition ot : tsNeedRemoved)
        {
            transitions.remove(ot);
        }
    }
    
}
