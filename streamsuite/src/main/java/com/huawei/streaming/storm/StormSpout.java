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

package com.huawei.streaming.storm;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.huawei.streaming.application.StreamAdapter;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IRichOperator;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 * Storm的spout
 *
 */
public class StormSpout implements IRichSpout, StreamAdapter
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 5995117604767005749L;
    
    private static final Logger LOG = LoggerFactory.getLogger(StormSpout.class);
    
    private IRichOperator input;
    
    private static final TupleEvent EMPTY_ENENT = new TupleEvent();
    
    private static final String SPOUT_STREAM = "SpoutStream";
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setOperator(IRichOperator operator)
    {
        if (operator == null)
        {
            LOG.error("Failed to set operator, operator is null.");
            throw new RuntimeException("failed to set operator");
        }
        input = operator;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        LOG.info("Start to open storm spout.");
        Map<String, IEmitter> emitters = createEmitters(collector);
        
        try
        {
            input.initialize(emitters);
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to initialize input stream.");
            throw new RuntimeException("Failed to initialize output stream", e);
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        LOG.debug("Start to close storm spout.");
        try
        {
            input.destroy();
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to destroy input stream.");
            throw new RuntimeException("Failed to destroy input stream", e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void activate()
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void deactivate()
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void nextTuple()
    {
        LOG.debug("Start to execute nextTuple from storm spout.");
        try
        {
            input.execute(SPOUT_STREAM, EMPTY_ENENT);
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to execute input stream.");
            throw new RuntimeException("Failed to execute input stream.", e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(Object msgId)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void fail(Object msgId)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        if (!StringUtils.isEmpty(input.getOutputStream()))
        {
            declarer.declareStream(input.getOutputStream(), new Fields(input.getOutputSchema().getAllAttributeNames()));
        }
        else
        {
            if (input.getOutputSchema() != null)
            {
                declarer.declare(new Fields(input.getOutputSchema().getAllAttributeNames()));
            }
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        return input.getConfig();
    }
    
    private Map<String, IEmitter> createEmitters(SpoutOutputCollector collector)
    {
        Map<String, IEmitter> emitters = Maps.newHashMap();
        SpoutEmitter emitter = new SpoutEmitter(collector, input.getOutputStream());
        emitters.put(input.getOutputStream(), emitter);
        
        return emitters;
    }
    
}
