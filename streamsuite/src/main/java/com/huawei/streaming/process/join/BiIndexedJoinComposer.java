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
 * 双数据流JOIN基本操作
 * <功能详细描述>
 * 
 */
public abstract class BiIndexedJoinComposer implements IJoinComposer
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -7115645738155982129L;
    
    private static final Logger LOG = LoggerFactory.getLogger(BiIndexedJoinComposer.class);
    
    private static final int STREAM_NUM = 2;
    
    private IIndexedEventCollection leftStream;
    
    private IIndexedEventCollection rightStream;
    
    private LinkedHashSet<MultiKey> oldResults = new LinkedHashSet<MultiKey>();
    
    private LinkedHashSet<MultiKey> newResults = new LinkedHashSet<MultiKey>();
    
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
    public BiIndexedJoinComposer(IIndexedEventCollection left, IIndexedEventCollection right, boolean rStream)
    {
        this.leftStream = left;
        this.rightStream = right;
        this.joinRStream = rStream;
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
        //默认0下标为左流，1下标为右流
        leftStream.addRemove(newDataPerStream[0], oldDataPerStream[0]);
        rightStream.addRemove(newDataPerStream[1], oldDataPerStream[1]);
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
            LOG.error("This updated streams numbers do NOT match with Bi Stream Join.");
            throw new RuntimeException("This updated streams numbers do NOT match with Bi Stream Join.");
        }
        
        newResults.clear();
        oldResults.clear();
        
        if (isJoinRStream())
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
        //保证结果中任一个都 非NULL
        return new Pair<Set<MultiKey>, Set<MultiKey>>(newResults, oldResults);
    }
    
    /**
     * 将更新数据与窗口中的对方流数据,根据KEY（条件）进行匹配
     * 根据是否允许Join 空流，由具体的Composer进行操作
     */
    protected void compose(IEvent[] events, int streamIndex, Set<MultiKey> result)
    {
        if (events == null || events.length == 0)
        {
            return;
        }
        
        ArrayDeque<IEvent[]> joinTemp = new ArrayDeque<IEvent[]>();
        for (IEvent theEvent : events)
        {
            perEventCompose(theEvent, streamIndex, joinTemp);
            
            // 如果joinTemp结果无事件，则没有任何结果
            for (IEvent[] row : joinTemp)
            {
                result.add(new MultiKey(row));
            }
            joinTemp.clear();
        }
    }
    
    /**
     * 根据条件（有索引数据）进行JOIN，无匹配事件，不输出
     * <功能详细描述>
     */
    protected void perEventCompose(IEvent lookupEvent, int index, Collection<IEvent[]> result)
    {
        if (null == lookupEvent)
        {
            return;
        }
        
        Set<IEvent> joinedEvents = getMatchEvents(lookupEvent, index);
        
        if (joinedEvents == null)
        {
            return;
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
     * 获得对方流中匹配事件
     * <功能详细描述>
     */
    protected abstract Set<IEvent> getMatchEvents(IEvent lookupEvent, int index);
    
    /**
     * {@inheritDoc}
     */
    public int getStreamsSize()
    {
        return STREAM_NUM;
    }
    
    protected Set<MultiKey> getOldResults()
    {
        return oldResults;
    }
    
    protected Set<MultiKey> getNewResults()
    {
        return newResults;
    }
    
    protected boolean isJoinRStream()
    {
        return joinRStream;
    }
    
    protected IIndexedEventCollection getLeftStream()
    {
        return leftStream;
    }
    
    protected IIndexedEventCollection getRightStream()
    {
        return rightStream;
    }
    
}
