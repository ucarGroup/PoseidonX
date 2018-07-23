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

package com.huawei.streaming.operator.outputstream;

import com.huawei.streaming.config.KafkaConfig;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IOutputStreamOperator;
import com.huawei.streaming.serde.BaseSerDe;
import com.huawei.streaming.serde.StreamSerDe;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 基于kafka的处理，完成Storm中的bolt的功能
 */
public class KafkaFunctionOp implements IOutputStreamOperator
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -3954667394060365585L;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFunctionOp.class);

    private transient Producer<Integer, String> producer;

    private String topic;

    private Properties kafkaProperties;

    private StreamSerDe serde;

    private StreamingConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf)
        throws StreamingException
    {
        kafkaProperties = new Properties();
        kafkaProperties.put(KafkaConfig.KAFKA_PRO_ZK_CONNECT,
            conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZOOKEEPERS));
        kafkaProperties.put(KafkaConfig.KAFKA_SERIAL_CLASS,
            conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_MESSAGESERIALIZERCLASS));
        kafkaProperties.put(KafkaConfig.KAFKA_SESSION_TIME,
            conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZKSESSIONTIMEOUT));
        kafkaProperties.put(KafkaConfig.KAFKA_SYNC_TIME,
            conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZKSYNCTIME));
        kafkaProperties.put(KafkaConfig.KAFKA_BROKER_LIST, conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_BROKERS));
        topic = conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_TOPIC);
        this.config = conf;
    }

    @Override
    public StreamingConfig getConfig()
    {
        return config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {
        producer = new Producer<Integer, String>(new ProducerConfig(kafkaProperties));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        String result = null;
        try
        {
            result = (String)serde.serialize(BaseSerDe.changeEventsToList(event));
        }
        catch (StreamSerDeException e)
        {
            LOG.warn("Ignore a serde exception.", e);
        }

        if (result == null)
        {
            LOG.warn("Ignore a null result in output.");
            return;
        }

        LOG.debug("The Output result is {}.", result);
        producer.send(new KeyedMessage<Integer, String>(topic, result));
        LOG.debug("Kafka send success.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        if (producer != null)
        {
            producer.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSerDe(StreamSerDe streamSerDe)
    {
        this.serde = streamSerDe;
    }

    @Override
    public StreamSerDe getSerDe()
    {
        return serde;
    }
}
