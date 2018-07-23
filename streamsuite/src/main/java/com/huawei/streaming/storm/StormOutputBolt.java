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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.application.StreamAdapter;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IRichOperator;
import com.huawei.streaming.operator.OutputOperator;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * Storm的输出bolt
 *
 */
public class StormOutputBolt implements IRichBolt, StreamAdapter
{

    private static final long serialVersionUID = -5921149658363414958L;

    private static final Logger LOG = LoggerFactory.getLogger(StormOutputBolt.class);

    private OutputCollector outputCollector;

    private OutputOperator output;

    private boolean needAck = false;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setOperator(IRichOperator operator)
    {
        this.output = (OutputOperator)operator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        LOG.debug("Start to prepare storm output bolt.");
        outputCollector = collector;
        if (null != output.getConfig().get(StreamingConfig.STREAMING_STORM_COMMON_ISACK))
        {
            needAck = Boolean.valueOf(output.getConfig().get(StreamingConfig.STREAMING_STORM_COMMON_ISACK).toString());
        }
        try
        {
            output.initialize();
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to initialize output stream.");
            throw new RuntimeException("Failed to initialize output stream", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple tuple)
    {
        LOG.debug("Start up to execute tuple.");
        String sourceStreamName = tuple.getSourceStreamId();
        try
        {
            for (String streamName : output.getInputStream())
            {
                if (sourceStreamName.equals(streamName))
                {
                    TupleEvent event = com.huawei.streaming.storm.TupleTransform.tupeToEvent(tuple, output.getInputSchema().get(streamName));
                    output.execute(streamName, event);
                }
            }
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to execute tuple.");
            throw new RuntimeException("Failed to execute tuple.", e);
        }

        if (needAck)
        {
            outputCollector.ack(tuple);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup()
    {
        LOG.debug("Start to cleanup storm output bolt.");

        try
        {
            output.destroy();
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to destroy output.");
            throw new RuntimeException("Failed to destroy output", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        return output.getConfig();
    }
}
