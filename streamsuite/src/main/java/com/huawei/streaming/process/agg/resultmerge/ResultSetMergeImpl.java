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

package com.huawei.streaming.process.agg.resultmerge;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.LimitProcess;
import com.huawei.streaming.process.OrderBySubProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <聚合操作结果合并抽象类>
 * 
 */
public abstract class ResultSetMergeImpl implements IAggResultSetMerge
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -7279395761701554251L;
    /**
     * 排序处理对象
     */
    private final OrderBySubProcess order;
    
    /**
     * 限量处理对象
     */
    private final LimitProcess limit;
    
    /**
     * 是否单向
     */
    private boolean unidirection = true;
    
    /**
     * <默认构造函数>
     *@param order 排序操作
     *@param limit 限量操作
     */
    public ResultSetMergeImpl(OrderBySubProcess order, LimitProcess limit)
    {
        this.order = order;
        this.limit = limit;
    }
    
    public void setUnidirection(boolean uni)
    {


        this.unidirection = uni;
    }
    
    public boolean getUnidirection()
    {
        return this.unidirection;
    }
    
    /**
     * <结果排序并根据输出限量，得到新的结果集>
     * <对结果集进行排序，然后根据输出限量。>
     */
    public Pair<IEvent[], IEvent[]> orderAndLimit(IEvent[] selectNewEvents, IEvent[] selectOldEvents, OutputType type)
    {
        //如果存在orderby，则对select结果进行排序。       
        if (null != this.order)
        {
            if (type != OutputType.I)
            {
                selectOldEvents = order.process(selectOldEvents);
            }
            selectNewEvents = order.process(selectNewEvents);
        }
        
        //如果存在limit，则执行limit操作，如果limit数据比结果集大则只输出结果集
        if (null != limit)
        {
            if (type != OutputType.I)
            {
                selectOldEvents = limit.process(selectOldEvents);
            }
            selectNewEvents = limit.process(selectNewEvents);
        }
        
        //返回排序限量后的结果
        if ((selectNewEvents == null) && (selectOldEvents == null))
        {
            return null;
        }
        
        return new Pair<IEvent[], IEvent[]>(selectNewEvents, selectOldEvents);
    }

}
