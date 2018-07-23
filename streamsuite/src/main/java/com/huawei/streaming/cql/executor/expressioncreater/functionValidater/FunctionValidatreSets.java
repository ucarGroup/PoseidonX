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

package com.huawei.streaming.cql.executor.expressioncreater.functionValidater;

import java.util.Set;

import com.google.common.collect.Sets;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionExpressionDesc;
import com.huawei.streaming.expression.IExpression;

/**
 * 函数验证功能集合
 * 
 */
public class FunctionValidatreSets implements FunctionValidater
{
    
    private Set<FunctionValidater> validaterSets = Sets.newHashSet();
    {
        validaterSets.add(new ToDateValidater());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(String functionName, IExpression[] argumentExpressions,
        FunctionExpressionDesc functionExpressionDesc)
    {
        for (FunctionValidater validator : validaterSets)
        {
            boolean result = validator.validate(functionName, argumentExpressions, functionExpressionDesc);
            if (!result)
            {
                return false;
            }
        }
        return true;
    }
}
