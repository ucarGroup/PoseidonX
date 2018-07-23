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

package com.huawei.streaming.api.opereators;

import com.huawei.streaming.api.ConfigAnnotation;
import com.huawei.streaming.config.StreamingConfig;

/**
 * kafka数据读取
 *
 */
public class KafkaOutputOperator extends InnerOutputSourceOperator
{
    /**
     * Topic
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_KAFKA_TOPIC)
    private String topic;

    /**
     * kafka读取数据的zookeeper地址
     * 地址加端口，多个之间用逗号分隔
     * 比如：192.168.0.2:2181,192.168.0.3:2181
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_KAFKA_ZOOKEEPERS)
    private String zookeepers;

    /**
     * brokers 列表
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_KAFKA_BROKERS)
    private String brokers;

    /**
     * zookeeper连接超时时间
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_KAFKA_ZKSESSIONTIMEOUT)
    private Integer zkSessionTimeout;

    /**
     * kafka zk 同步时间参数
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_KAFKA_ZKSYNCTIME)
    private Integer zkSyncTime;

    /**
     * kafka消费者中获取到数据之后的序列化类
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_KAFKA_MESSAGESERIALIZERCLASS)
    private String messageSerializerClass;

    /**
     * <默认构造函数>
     *
     */
    public KafkaOutputOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }

    public String getBrokers()
    {
        return brokers;
    }

    public void setBrokers(String brokers)
    {
        this.brokers = brokers;
    }

    public String getZookeepers()
    {
        return zookeepers;
    }

    public void setZookeepers(String zookeepers)
    {
        this.zookeepers = zookeepers;
    }

    public Integer getZkSessionTimeout()
    {
        return zkSessionTimeout;
    }

    public void setZkSessionTimeout(Integer zkSessionTimeout)
    {
        this.zkSessionTimeout = zkSessionTimeout;
    }

    public Integer getZkSyncTime()
    {
        return zkSyncTime;
    }

    public void setZkSyncTime(Integer zkSyncTime)
    {
        this.zkSyncTime = zkSyncTime;
    }

    public String getMessageSerializerClass()
    {
        return messageSerializerClass;
    }

    public void setMessageSerializerClass(String messageSerializerClass)
    {
        this.messageSerializerClass = messageSerializerClass;
    }

    public String getTopic()
    {
        return topic;
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

}
