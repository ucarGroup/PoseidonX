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

package com.huawei.streaming.process.join;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.event.IEvent;

/**
 * InnerJoin数据组合
 * 必须有ON条件：必须为等值条件，属性值匹配，可以多个属性值等式
 * 无匹配时，不输出
 * 
 */
public class InnerBiJoinComposer extends BiIndexedJoinComposer
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -1311187964479951552L;
    
    private static final Logger LOG = LoggerFactory.getLogger(InnerBiJoinComposer.class);
    
    /**
     * <默认构造函数>
     *@param left  左流有效数据
     *@param right 右流有效数据
     *@param rStream 是否计算RSTREAM
     */
    public InnerBiJoinComposer(IIndexedEventCollection left, IIndexedEventCollection right, boolean rStream)
    {
        super(left, right, rStream);
        LOG.debug("Initiate InnerBiJoinComposer. Left stream name={}, Right stream name={}, joinRStream={}",
            new Object[] {left.getStreamName(), right.getStreamName(), rStream});
    }
    
    /**
     * {@inheritDoc}
     * 重载
     * 如果无匹配事件，返回空
     */
    protected Set<IEvent> getMatchEvents(IEvent lookupEvent, int index)
    {
        Set<IEvent> joinedEvents;
        MultiKey lookupKey = null;
        if (index == 0) //当前数据为left流，与right流JOIN
        {
            lookupKey = getLeftStream().getIndex(lookupEvent);
            joinedEvents = getRightStream().lookup(lookupKey);
        }
        else
        //当前数据为right流，与left流JOIN
        {
            lookupKey = getRightStream().getIndex(lookupEvent);
            joinedEvents = getLeftStream().lookup(lookupKey);
        }
        return joinedEvents;
    }
}
