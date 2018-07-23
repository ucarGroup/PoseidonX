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
import com.huawei.streaming.process.agg.resultmerge.IResultSetMerge;

/**
 * <聚合处理器>
 * 
 */
public class AggregateProcessor extends ProcessorImpl
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -5739187201725551246L;
    
    /**
     * 结果合并
     */
    private final IResultSetMerge resultSetMerge;
    
    /**
     * 结果输出
     */
    private final IOutput output;
    
    /**
     * 输出类型
     */
    private final OutputType type;
    
    /**
     * <默认构造函数>
     *@param resultSetMerge 结果合并
     *@param output 结果输出       
     *@param type 输出类型
     */
    public AggregateProcessor(IResultSetMerge resultSetMerge, IOutput output, OutputType type)
    {
        this.resultSetMerge = resultSetMerge;
        this.output = output;
        this.type = type;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void process(IEvent[] newData, IEvent[] oldData)
    {
        //计算结果
        Pair<IEvent[], IEvent[]> newOldEvents = resultSetMerge.processResult(newData, oldData, type);
        
        //输出结果
        output.output(newOldEvents);
    }
    
}
