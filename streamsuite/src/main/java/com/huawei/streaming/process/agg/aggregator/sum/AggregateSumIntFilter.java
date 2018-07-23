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

package com.huawei.streaming.process.agg.aggregator.sum;

import com.huawei.streaming.process.agg.aggregator.AggregateFilterUtil;

/**
 * 
 * Integer类型条件Sum算子
 *
 * 
 */
class AggregateSumIntFilter extends AggregateSumInt
{
    /**
     * ID
     */
    private static final long serialVersionUID = 238098380804773438L;
    
    @Override
    public void enter(Object value, boolean filter)
    {
        if (!AggregateFilterUtil.checkFilter(filter))
        {
            return;
        }
        
        super.enter(value);
    }
    
    @Override
    public void leave(Object value, boolean filter)
    {
        if (!AggregateFilterUtil.checkFilter(filter))
        {
            return;
        }
        
        super.leave(value);
    }
}
