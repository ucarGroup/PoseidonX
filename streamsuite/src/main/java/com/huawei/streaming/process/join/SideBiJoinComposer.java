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
 * left/right Join处理
 * 必须有ON条件：必须为等值条件，属性值匹配，可以多个属性值等式
 * 根据左流/右流条件，无匹配事件时，输出可为带空事件的匹配
 * 
 */
public class SideBiJoinComposer extends BiIndexedJoinComposer
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 6685903301221320055L;
    
    private static final Logger LOG = LoggerFactory.getLogger(SideBiJoinComposer.class);
    
    private SideJoinType sideJoinType;
    
    /**
     * <默认构造函数>
     *@param left  左流有效数据
     *@param right 右流有效数据
     *@param joinType JOIN类型：左流JOINor右流JOIN
     *@param rStream 是否计算RSTREAM
     */
    public SideBiJoinComposer(IIndexedEventCollection left, IIndexedEventCollection right, SideJoinType joinType,
        boolean rStream)
    {
        super(left, right, rStream);
        this.sideJoinType = joinType;
        LOG.debug("Initiate SideBiJoinComposer. "
            + "Left stream name={}, Right stream name={}, SideJoinType={}, joinRStream={}.",
            new Object[] {left.getStreamName(), right.getStreamName(), joinType, rStream});
    }
    
    /**
     * {@inheritDoc}
     * 针对左流JOIN或者右流JOIN，如果无匹配事件，返回一个属性值全为空的事件
     */
    @Override
    protected Set<IEvent> getMatchEvents(IEvent lookupEvent, int index)
    {
        Set<IEvent> joinedEvents;
        MultiKey lookupKey = null;
        if (index == 0) //当前数据为left流，与right流JOIN
        {
            lookupKey = getLeftStream().getIndex(lookupEvent);
            switch (sideJoinType)
            {
                case LEFTJOIN: // 左流JOIN， 如果无匹配事件，返回一个属性值全为空的事件
                    joinedEvents = getRightStream().lookupWithNull(lookupKey);
                    break;
                case RIGHTJOIN: // 右流JOIN， 如果无匹配事件，返回空
                    joinedEvents = getRightStream().lookup(lookupKey);
                    break;
                default:
                    LOG.error("The side join type is not supported. join type={}.", sideJoinType);
                    throw new RuntimeException("The side join type is not supported.");
            }
        }
        else
        //当前数据为right流，与left流JOIN        
        {
            lookupKey = getRightStream().getIndex(lookupEvent);
            switch (sideJoinType)
            {
                case LEFTJOIN: // 左流JOIN， 如果无匹配事件，返回空 
                    joinedEvents = getLeftStream().lookup(lookupKey);
                    break;
                case RIGHTJOIN: // 右流JOIN， 如果无匹配事件，  返回一个属性值全为空的事件
                    joinedEvents = getLeftStream().lookupWithNull(lookupKey);
                    break;
                default:
                    LOG.error("The side join type is not supported. join type={}.", sideJoinType);
                    throw new RuntimeException("The side join type is not supported.");
            }
        }
        return joinedEvents;
    }
}
