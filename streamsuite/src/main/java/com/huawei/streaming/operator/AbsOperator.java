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

package com.huawei.streaming.operator;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.application.GroupInfo;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;

/**
 * 基础的流处理算子实现类
 * 只实现了基本的并发设置，参数设置等方法
 * <p/>
 * Streaming内部实现都依赖于此类
 * 外部Storm相关不感知此类
 *
 */
public abstract class AbsOperator implements IRichOperator
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 9152942480213961636L;
    
    private StreamingConfig conf = null;
    
    private int parallelNumber;
    
    private String operatorId;
    
    private Map<String, GroupInfo> groupInfos;
    
    private Map<String, IEmitter> emitters;
    
    /**
     * <默认构造函数>
     */
    public AbsOperator()
    {
        groupInfos = Maps.newHashMap();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final void initialize(Map<String, IEmitter> emitterMap)
        throws StreamingException
    {
        this.emitters = emitterMap;
        initialize();
    }

    /**
     * 初始化
     *
     */
    public abstract void initialize()
        throws StreamingException;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig config) throws StreamingException
    {
        this.conf = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamingConfig getConfig()
    {
        return conf;
    }

    /**
     * 设置输入流名称
     *
     */
    public abstract void setInputStream(List<String> streamNames)
        throws StreamingException;
    
    /**
     * 设置输出流名称
     *
     */
    public abstract void setOutputStream(String streamName)
        throws StreamingException;
    
    /**
     * 设置输入schema，和输入流名称一一对应
     *
     */
    public abstract void setInputSchema(Map<String, IEventType> schemas)
        throws StreamingException;
    
    /**
     * 设置输出schema
     *
     */
    public abstract void setOutputSchema(IEventType schema)
        throws StreamingException;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getOperatorId()
    {
        return this.operatorId;
    }
    
    /**
     * 设置算子id
     *
     */
    public void setOperatorId(String id)
    {
        operatorId = id;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getParallelNumber()
    {
        return parallelNumber;
    }
    
    /**
     * 设置算子并发度
     *
     */
    public void setParallelNumber(int number)
    {
        this.parallelNumber = number;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, GroupInfo> getGroupInfo()
    {
        return groupInfos;
    }
    
    /**
     * 设置算子分组信息
     *
     */
    public void setGroupInfo(Map<String, GroupInfo> groups)
    {
        groupInfos = groups;
    }
    
    /**
     * 设置算子分组信息
     *
     */
    public void setGroupInfo(String streamName, DistributeType distributeType, String[] fields)
    {
        GroupInfo groupInfo = new GroupInfo();
        groupInfo.setStreamName(streamName);
        groupInfo.setDitributeType(distributeType);
        
        if (null != fields && fields.length > 0)
        {
            //设置分发的时候，如果有多个列，只设置使用第一个
            //这也就要求，在使用cql的时候，group by的第一列应该尽可能的分布均匀。
            List<String> f = Lists.newArrayList(fields[0]);
            groupInfo.setFields(f);
        }
        
        groupInfos.put(groupInfo.getStreamName(), groupInfo);
    }
    
    /**
     * 通过流名称获取emitter
     *
     */
    public Map<String, IEmitter> getEmitterMap()
    {
       return emitters;
    }
    
    /**
     * 通过流名称获取emitter
     *
     */
    public IEmitter getEmitter(String streamName)
    {
        if (emitters.containsKey(streamName))
        {
            return emitters.get(streamName);
        }
        throw new StreamingRuntimeException("can not get emitter by stream name " + streamName);
    }
    
    /**
     * 通过流名称获取emitter
     *
     */
    public IEmitter getEmitter()
    {
        if (emitters.containsKey(getOutputStream()))
        {
            return emitters.get(getOutputStream());
        }
        throw new StreamingRuntimeException("can not get emitter by stream name " + this.getOutputStream());
    }
    
}
