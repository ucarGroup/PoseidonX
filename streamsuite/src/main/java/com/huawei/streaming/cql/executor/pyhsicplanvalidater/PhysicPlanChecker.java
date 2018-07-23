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

package com.huawei.streaming.cql.executor.pyhsicplanvalidater;

import java.util.ArrayList;
import java.util.List;

import com.huawei.streaming.api.Application;
import com.huawei.streaming.cql.exception.ExecutorException;

/**
 * 在执行计划完成之间进行检查
 * 
 * TODO 各类算子参数是否允许为空的检查，算子参数有效性的检查
 * 
 */
public class PhysicPlanChecker
{
    private List<Validater> validaters = new ArrayList<Validater>();
    
    /**
     * <默认构造函数>
     */
    public PhysicPlanChecker()
    {
        validaters.add(new TransitionRepeatValidater());
        validaters.add(new OperatorParmetersValueValidate());
        validaters.add(new OperatorParmetersIsNullValidater());
    }
    
    /**
     * 检查物理执行计划
     */
    public void check(Application app)
        throws ExecutorException
    {
        for (Validater v : validaters)
        {
            v.validate(app);
        }
    }
}
