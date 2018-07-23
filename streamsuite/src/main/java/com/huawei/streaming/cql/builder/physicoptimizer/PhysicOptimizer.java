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

import com.huawei.streaming.api.Application;
import com.huawei.streaming.cql.exception.ApplicationBuildException;

/**
 * 物理优化器
 * 
 * 优化内容：
 * 1、OrderBy优化，实现sorted-merge排序。
 * 2、limit优化，上一个算子中加入limit。
 * 3、算子替换，将功能比较简单的算子，替换为Filter算子或者functor算子
 * 4、移除无意义的filter算子
 * 
 */
public class PhysicOptimizer implements Optimizer
{
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Application optimize(Application app)
        throws ApplicationBuildException
    {
        new AggregateConverter().optimize(app);
        new FilterPruner().optimize(app);
        new SameStreamCombiner().optimize(app);
        new SameTransitionPruner().optimize(app);
        return app;
    }
}
