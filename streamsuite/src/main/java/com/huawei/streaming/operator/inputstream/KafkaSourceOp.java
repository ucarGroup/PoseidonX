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
package com.huawei.streaming.operator.inputstream;

import com.google.common.collect.Maps;
import com.huawei.streaming.config.KafkaConfig;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IInputStreamOperator;
import com.huawei.streaming.serde.StreamSerDe;
import kafka.api.OffsetRequest;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 基于Kafka的处理，完成Storm中的Spout功能
 * --2015.10.31
 * 基于Storm-kafka对该算子进行修改。
 */
public class KafkaSourceOp implements IInputStreamOperator
{

    /**
     * 序列化id
     */
    private static final long serialVersionUID = 4659402040319781576L;

    /**
     * 日志对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceOp.class);

    private StreamingConfig config;

    private StreamSerDe serde;

    /**
     * 字符编解码格式
     */
    private static final Charset UTF8 = Charset.forName("UTF-8");

    /**
     * topic map中该topic对应的数量
     */
    private static final int TOPIC_COUNT = 1;

    /**
     * kafka消费者连接信息
     */
    private transient ConsumerConnector consumerConnector;

    /**
     * 消息
     */
    private transient ConsumerIterator<byte[], byte[]> consumerIterator;

    private Properties kafkaProperties;

    private String topic;

    private IEmitter emitter;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf)
        throws StreamingException
    {
        config = conf;
        kafkaProperties = initKafkaProperties(conf);
        topic = conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_TOPIC);
    }

    /**
     * {@inheritDoc}
     */
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
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProperties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = Maps.newHashMap();
        topicCountMap.put(topic, TOPIC_COUNT);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
            consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        consumerIterator = stream.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute()
        throws StreamingException
    {
        byte[] bytes = consumerIterator.next().message();

        List<Object[]> vals = null;
        try
        {
            vals = serde.deSerialize(new String(bytes, UTF8));
        }
        catch (StreamSerDeException e)
        {
            LOG.warn("Ignore a serde exception.", e);
            return;
        }

        if (null == vals)
        {
            return;
        }

        for (Object[] value : vals)
        {
            emitter.emit(value);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        if (consumerConnector != null)
        {
            consumerConnector.shutdown();
        }
    }

    private Properties initKafkaProperties(StreamingConfig conf)
        throws StreamingException
    {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(KafkaConfig.KAFKA_CON_ZK_CONNECT,
            conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZOOKEEPERS));
        kafkaProperties.put(KafkaConfig.KAFKA_GROUP_ID, conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_GROUPID));
        kafkaProperties.put(KafkaConfig.KAFKA_SERIAL_CLASS,
            conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_MESSAGESERIALIZERCLASS));
        kafkaProperties.put(KafkaConfig.KAFKA_SESSION_TIME,
            conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZKSESSIONTIMEOUT));
        kafkaProperties.put(KafkaConfig.KAFKA_SYNC_TIME,
            conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZKSYNCTIME));

        if (conf.getBooleanValue(StreamingConfig.OPERATOR_KAFKA_READ_FROMBEGINNING))
        {
            kafkaProperties.put(KafkaConfig.KAFKA_OFFSET_RESET, OffsetRequest.SmallestTimeString());
        }
        else
        {
            kafkaProperties.put(KafkaConfig.KAFKA_OFFSET_RESET, OffsetRequest.LargestTimeString());

        }

        return kafkaProperties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEmitter(IEmitter iEmitter)
    {
        this.emitter = iEmitter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSerDe(StreamSerDe streamSerDe)
    {
        serde = streamSerDe;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamSerDe getSerDe()
    {
        return serde;
    }
}
