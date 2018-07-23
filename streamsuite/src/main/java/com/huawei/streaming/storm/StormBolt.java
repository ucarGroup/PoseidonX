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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.huawei.streaming.application.StreamAdapter;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IRichOperator;
import com.huawei.streaming.operator.functionstream.SplitOp;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Storm的bolt
 *
 */
public class StormBolt implements IRichBolt, StreamAdapter
{
    private static final long serialVersionUID = -5921149658363414958L;
    
    private static final Logger LOG = LoggerFactory.getLogger(StormBolt.class);
    
    private IRichOperator functionStream;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setOperator(IRichOperator operator)
    {
        functionStream = operator;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        Map<String, IEmitter> emitters = createEmitters(collector);
        try
        {
            functionStream.initialize(emitters);
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to initialize function stream.");
            throw new RuntimeException("failed to initialize output stream", e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input)
    {
        LOG.debug("start to execute storm bolt");
        if (input == null)
        {
            LOG.error("Input tuple is null.");
            throw new RuntimeException("Input tuple is null.");
        }
        
        String sourceStreamName = input.getSourceStreamId();
        
        if (StringUtils.isEmpty(sourceStreamName))
        {
            LOG.error("sourceStreamName is null.");
            throw new RuntimeException("sourceStreamName is nul");
        }
        
        try
        {
            List<String> inStreams = functionStream.getInputStream();
            if(inStreams == null)
            {
                LOG.error("inStreams is null.");
                throw new RuntimeException("inStreams is nul");
            }
            
            for (String streamName : inStreams)
            {
                if (!sourceStreamName.equals(streamName))
                {
                    continue;
                }
                
                TupleEvent event = TupleTransform.tupeToEvent(input, functionStream.getInputSchema().get(streamName));
                functionStream.execute(streamName, event);
            }
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to execute tuple.");
            throw new RuntimeException("Failed to execute tuple.", e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup()
    {
        try
        {
            functionStream.destroy();
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to destroy function stream.");
            throw new RuntimeException("Failed to destroy function stream", e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        if (functionStream instanceof SplitOp)
        {
            SplitOp split = (SplitOp)functionStream;
            Map<String, IEventType> streamNames = split.getOutputSchemaMap();
            if (streamNames == null)
            {
                LOG.error("Failed to get output stream.");
                throw new RuntimeException("Failed to get output stream.");
            }
            for (Entry<String, IEventType> et : streamNames.entrySet())
            {
                String streamName = et.getKey();
                IEventType schema = et.getValue();
                declare(declarer, streamName, schema);
            }
        }
        else
        {
            IEventType schema = functionStream.getOutputSchema();
            if (schema == null)
            {
                LOG.error("Failed to get output stream schema.");
                throw new RuntimeException("Failed to get output stream schema.");
            }
            declare(declarer, functionStream.getOutputStream(), schema);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        return functionStream.getConfig();
    }
    
    private Map<String, IEmitter> createEmitters(OutputCollector collector)
    {
        Map<String, IEmitter> emitters = Maps.newHashMap();
        if (functionStream instanceof SplitOp)
        {
            createEmitterForSplit(collector, emitters);
        }
        else
        {
            BoltEmitter emitter = new BoltEmitter(collector, functionStream.getOutputStream());
            emitters.put(functionStream.getOutputStream(), emitter);
        }
        return emitters;
    }
    
    private void createEmitterForSplit(OutputCollector collector, Map<String, IEmitter> emitters)
    {
        SplitOp split = (SplitOp)functionStream;
        Map<String, IEventType> streamNames = split.getOutputSchemaMap();
        for (String stream : streamNames.keySet())
        {
            BoltEmitter emit = new BoltEmitter(collector, stream);
            emitters.put(stream, emit);
        }
    }
    
    private void declare(OutputFieldsDeclarer declarer, String streamName, IEventType schema)
    {
        if (!StringUtils.isEmpty(streamName))
        {
            declarer.declareStream(streamName, new Fields(schema.getAllAttributeNames()));
        }
        else
        {
            declarer.declare(new Fields(schema.getAllAttributeNames()));
        }
    }
    
}
