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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;

/**
 * 
 * 双数据流Cross Join操作
 * 无等值条件，可以有过滤条件
 * 匹配所有事件
 * 无匹配事件时，输出可为带空事件的匹配
 * 
 */
public class CrossBiJoinComposer implements IJoinComposer
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -705647270478065074L;
    
    private static final Logger LOG = LoggerFactory.getLogger(CrossBiJoinComposer.class);
    
    private static final int STREAM_NUM = 2;
    
    private SimpleEventCollection leftStream;
    
    private SimpleEventCollection rightStream;
    
    private Set<MultiKey> oldResults = new LinkedHashSet<MultiKey>();
    
    private Set<MultiKey> newResults = new LinkedHashSet<MultiKey>();
    
    /**
     * 是否计算RSTREAM
     */
    private boolean joinRStream;
    
    /**
     * <默认构造函数>
     *@param left  左流有效数据
     *@param right 右流有效数据
     *@param rStream 是否计算RSTREAM
     */
    public CrossBiJoinComposer(SimpleEventCollection left, SimpleEventCollection right, boolean rStream)
    {
        LOG.debug("Initiate CrossBiJoinComposer. Left stream name={}, Right stream name={}, joinRStream={}.",
            new Object[] {left.getStreamName(), right.getStreamName(), rStream});
        this.leftStream = left;
        this.rightStream = right;
        this.joinRStream = rStream;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Pair<Set<MultiKey>, Set<MultiKey>> join(IEvent[][] newDataPerStream, IEvent[][] oldDataPerStream)
    {
        if (null == newDataPerStream && null == oldDataPerStream)
        {
            throw new RuntimeException("Mission impossible.");
        }
        
        if ((null != newDataPerStream && newDataPerStream.length != STREAM_NUM)
            || (null != oldDataPerStream && oldDataPerStream.length != STREAM_NUM))
        {
            LOG.error("This updated streams numbers is not for Cross Bi Stream Join.");
            throw new RuntimeException("This updated streams numbers is not for Bi Stream Join.");
        }
        
        newResults.clear();
        oldResults.clear();
        
        if (joinRStream)
        {
            if (null != oldDataPerStream)
            {
                for (int streamIndex = 0; streamIndex < oldDataPerStream.length; streamIndex++)
                {
                    compose(oldDataPerStream[streamIndex], streamIndex, oldResults);
                }
            }
        }
        
        if (null != newDataPerStream)
        {
            for (int streamIndex = 0; streamIndex < newDataPerStream.length; streamIndex++)
            {
                compose(newDataPerStream[streamIndex], streamIndex, newResults);
            }
        }
        
        return new Pair<Set<MultiKey>, Set<MultiKey>>(newResults, oldResults);
    }
    
    /**
     * 针对每个更新数据，与对方流有效数据进行全匹配
     * 如果对方 流中没有有效数据，至少输出一条结果数据
     */
    private void compose(IEvent[] events, int streamIndex, Set<MultiKey> result)
    {
        if (events == null || events.length == 0)
        {
            return;
        }
        
        ArrayDeque<IEvent[]> joinTemp = new ArrayDeque<IEvent[]>();
        for (IEvent theEvent : events)
        {
            perEventCompose(theEvent, streamIndex, joinTemp);
            
            for (IEvent[] row : joinTemp)
            {
                result.add(new MultiKey(row));
            }
            joinTemp.clear();
        }
        
    }
    
    /**
     * 进行全匹配JOIN
     * 如果joinTemp结果无事件，至少输出一条结果
     */
    private void perEventCompose(IEvent lookupEvent, int index, Collection<IEvent[]> result)
    {
        if (null == lookupEvent)
        {
            return;
        }
        
        Set<IEvent> joinedEvents;
        
        if (index == 0) //当前数据为left流，与right流JOIN
        {
            
            joinedEvents = rightStream.lookupAllWithNull();
        }
        else
        //当前数据为right流，与left流JOIN
        {
            joinedEvents = leftStream.lookupAllWithNull();
        }
        
        // Create result row for each found event
        for (IEvent joinedEvent : joinedEvents)
        {
            IEvent[] events = new IEvent[STREAM_NUM];
            events[index] = lookupEvent;
            events[STREAM_NUM - index - 1] = joinedEvent;
            result.add(events);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void maintainData(IEvent[][] newDataPerStream, IEvent[][] oldDataPerStream)
    {
        if (newDataPerStream.length != STREAM_NUM || oldDataPerStream.length != STREAM_NUM)
        {
            LOG.error("This updated streams numbers is not for Bi Stream Join.");
            throw new RuntimeException("This updated streams numbers is not for Bi Stream Join.");
        }
        
        leftStream.addRemove(newDataPerStream[0], oldDataPerStream[0]);
        rightStream.addRemove(newDataPerStream[1], oldDataPerStream[1]);
    }
    
    @Override
    public int getStreamsSize()
    {
        return STREAM_NUM;
    }
    
    public SimpleEventCollection getLeftStream()
    {
        return leftStream;
    }
    
    public SimpleEventCollection getRightStream()
    {
        return rightStream;
    }
    
}
