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

import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.lock.ILock;
import com.huawei.streaming.lock.LockImpl;
import com.huawei.streaming.output.IOutput;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.join.IJoinComposer;
import com.huawei.streaming.process.join.IJoinSetProcessor;
import com.huawei.streaming.process.join.JoinFilterProcessor;

/**
 * 两个数据流的Join操作
 * <功能详细描述>
 * 
 */
public class JoinProcessor extends ProcessorImpl
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -3565183563328255491L;
    
    private static final Logger LOG = LoggerFactory.getLogger(JoinProcessor.class);
    
    private IJoinComposer joinComposer;
    
    private JoinFilterProcessor joinFilter;
    
    private IJoinSetProcessor joinSetProcess;
    
    private final IOutput output;
    
    private final OutputType outputType;
    
    private String[] streamNames;
    
    private int streamNum;
    
    private boolean unidirectional = false;
    
    private int uniStreamIndex = 0;
    
    private boolean selfJoin = false;

    private boolean synFlag = false;

    /**
     * 锁对象，当前线程与定时器线程访问数据时需要先获取锁。
     */
    private ILock lock = new LockImpl();
    
    /**
     * 
     *  <默认构造函数>
     *@param comps      Join组合处理
     *@param names      Join流名称
     *@param filter     Join过滤处理
     *@param setProcess Join结果选择处理
     *@param out        Join输出对象
     *@param type       Join输出类型
     */
    public JoinProcessor(IJoinComposer comps, String[] names, JoinFilterProcessor filter, IJoinSetProcessor setProcess,
        IOutput out, OutputType type)
    {
        LOG.debug("Initiate JoinProcessor.");
        if (null == comps)
        {
            LOG.error("The join composer is null.");
            throw new RuntimeException("The join composer is null.");
        }
        
        if (null == setProcess)
        {
            LOG.error("The selct process is null.");
            throw new RuntimeException("The selct process is null.");
        }
        
        if (null == out)
        {
            LOG.error("The output process is null.");
            throw new RuntimeException("The output process is null.");
        }
        
        if (names.length != comps.getStreamsSize())
        {
            LOG.error("The streams number are not match. Composer size is {}, and stream names size is {}.",
                comps.getStreamsSize(),
                names.length);
            throw new RuntimeException("The streams number are not match.");
        }
        
        this.joinComposer = comps;
        this.joinFilter = filter;
        this.joinSetProcess = setProcess;
        this.streamNames = names;
        this.streamNum = names.length;
        this.output = out;
        if (null != type)
        {
            this.outputType = type;
        }
        else
        {
            this.outputType = OutputType.I;
        }
    }
    
    /**
     * 设置是否为单向
     * <功能详细描述>
     */
    public void setUnidirectional(Boolean uni)
    {
        this.unidirectional = uni;
    }
    
    /**
     * 设置单向流索引
     * <功能详细描述>
     */
    public void setUniStreamIndex(int index)
    {
        this.uniStreamIndex = index;
    }
    
    /**
     * 设置是否单流JOIN
     * <功能详细描述>
     */
    public void setSelfJoin(Boolean self)
    {
        this.selfJoin = self;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void process(IEvent[] newData, IEvent[] oldData)
    {
        IEvent event = getCurrentEvent(newData, oldData);
        if (null == event)
        {
            return;
        }
        
        try
        {
            lock.lock();
            
            //获得当前事件所属事件流的下标
            int streamIndex = getStreamIndex(event);
            
            //将更新数据按数据流组织
            IEvent[][] newDataPerStream = new IEvent[streamNum][];
            IEvent[][] oldDataPerStream = new IEvent[streamNum][];
            
            newDataPerStream[streamIndex] = newData;
            oldDataPerStream[streamIndex] = oldData;
            
            //维护多个流中的窗口有效数据
            joinComposer.maintainData(newDataPerStream, oldDataPerStream);
            
            /**
             * 如果为单流，通过开关保证数据都进两个窗口后进行JOIN操作,只进入一个窗口，返回
             */
            if (selfJoin)
            {
                if (synFlag)
                {
                    synFlag = false;
                }
                else
                {
                    synFlag = true;
                    return;
                }
            }

            //对事件JOIN
            Pair<Set<MultiKey>, Set<MultiKey>> composedEvents = null;
            //如果为单向
            if (unidirectional)
            {
                if (!selfJoin)
                {
                    if (streamIndex != uniStreamIndex)
                    {
                        return;
                    }
                }
            }
            
            composedEvents = joinComposer.join(newDataPerStream, oldDataPerStream);
            
            /*
             * 空时间判断，防止join之后产生空值
             */
            /*switch (outputType)
            {
                case I:
                    if (composedEvents.getFirst() == null || composedEvents.getFirst().size() == 0)
                    {
                        return;
                    }
                    break;
                case R:
                    if (composedEvents.getSecond() == null || composedEvents.getSecond().size() == 0)
                    {
                        return;
                    }
                    break;
                case IR:
                default:
                    if ((composedEvents.getFirst() == null || composedEvents.getFirst().size() == 0)
                        && (composedEvents.getSecond() == null || composedEvents.getSecond().size() == 0))
                    {
                        return;
                    }
                    break;
            }
            
            //对JOIN结果进行过滤
            
            if (null != joinFilter)
            {
                switch (outputType)
                {
                    case I:
                        joinFilter.filter(composedEvents.getFirst());
                        break;
                    case R:
                        joinFilter.filter(composedEvents.getSecond());
                        break;
                    case IR:
                        joinFilter.filter(composedEvents.getFirst());
                        joinFilter.filter(composedEvents.getSecond());
                        break;
                    default:
                        LOG.error("Not supported. output type={}", outputType);
                        throw new RuntimeException("Not supported.");
                }
            }
            
            //对过滤结果进行取值
            IEvent[] newResult = null;
            IEvent[] oldResult = null;
            switch (outputType)
            {
                case I:
                    newResult = joinSelect.process(composedEvents.getFirst());
                    break;
                case R:
                    oldResult = joinSelect.process(composedEvents.getSecond());
                    break;
                case IR:
                default:
                    newResult = joinSelect.process(composedEvents.getFirst());
                    oldResult = joinSelect.process(composedEvents.getSecond());
            }
            
            Pair<IEvent[], IEvent[]> out = new Pair<IEvent[], IEvent[]>(newResult, oldResult);
            */
            
            if (null != joinFilter)
            {
                joinFilter.filter(composedEvents.getFirst());
                joinFilter.filter(composedEvents.getSecond());
            }
            
            Pair<IEvent[], IEvent[]> out =
                joinSetProcess.processJoinResult(composedEvents.getFirst(), composedEvents.getSecond(), outputType);
            
            output.output(out);
        }
        
        finally
        {
            if (lock.isLocked())
            {
                lock.unlock();
            }
        }
        
    }
    
    /**
     * <返回事件对应类型在Join操作中索引>
     */
    private int getStreamIndex(IEvent event)
    {
        String name = event.getStreamName();
        for (int i = 0; i < streamNum; i++)
        {
            if (StringUtils.equals(name, streamNames[i]))
            {
                return i;
            }
        }
        LOG.error("Wrong stream name. name={}.", name);
        throw new RuntimeException("Wrong stream name.");
    }
    
    /**
     * <从新事件和旧事件中获取事件，如果新事件不为空，则从新事件获取，否则从旧事件中获取>
     */
    private IEvent getCurrentEvent(IEvent[] newData, IEvent[] oldData)
    {
        if (null == newData && null == oldData)
        {
            return null;
        }
        
        IEvent event = null;
        
        if (null != newData)
        {
            event = newData[0];
            if (null != event)
            {
                return event;
            }
        }
        return oldData[0];
    }
}
