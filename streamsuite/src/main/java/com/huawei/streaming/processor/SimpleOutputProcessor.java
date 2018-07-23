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

package com.huawei.streaming.processor;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.output.IOutput;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.OrderBySubProcess;
import com.huawei.streaming.process.SelectSubProcess;

/**
 * 
 * <简单选择处理器，选择内容仅包含事件属性>
 * 
 */
public class SimpleOutputProcessor extends ProcessorImpl
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 2154247189290113615L;
    
    private final SelectSubProcess selector;
    
    private final OrderBySubProcess order;
    
    private final IOutput output;
    
    private final OutputType type;
    
    /**
     * <默认构造函数>
     *@param select 选择操作
     *@param orderby 排序操作
     *@param output 输出对象
     *@param type 输出类型
     */
    public SimpleOutputProcessor(SelectSubProcess select, OrderBySubProcess orderby, IOutput output, OutputType type)
    {
        this.selector = select;
        this.order = orderby;
        this.output = output;
        this.type = type;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void process(IEvent[] newData, IEvent[] oldData)
    {
        IEvent[] newresult = getSelectEvents(selector, newData);
        IEvent[] oldresult = getSelectEvents(selector, oldData);
        
        if (null != order)
        {
            if (type != OutputType.I)
            {
                oldresult = order.process(oldresult);
            }
            newresult = order.process(newresult);
        }
        
        Pair<IEvent[], IEvent[]> result = new Pair<IEvent[], IEvent[]>(newresult, oldresult);
        
        output.output(result);
    }
    
    /**
     * <得到选择结果>
     */
    private IEvent[] getSelectEvents(SelectSubProcess select, IEvent[] newData)
    {
        return select == null ? newData : select.process(newData);
    }
}
