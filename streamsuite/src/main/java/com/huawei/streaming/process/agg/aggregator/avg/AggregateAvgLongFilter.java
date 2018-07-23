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

package com.huawei.streaming.process.agg.aggregator.avg;

import com.huawei.streaming.process.agg.aggregator.AggregateFilterUtil;

/**
 * Long类型条件Avg算子
 * 
 * 
 */
public class AggregateAvgLongFilter extends AggregateAvgLong
{
    
    /**
     * ID
     */
    private static final long serialVersionUID = -4796332607088283725L;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void enter(Object value, boolean filter)
    {
        if (!AggregateFilterUtil.checkFilter(filter))
        {
            return;
        }
        
        super.enter(value);
    }
    
    /**
     * {@inheritDoc}
     */
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
