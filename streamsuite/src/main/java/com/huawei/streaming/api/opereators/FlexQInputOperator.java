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
 * MetaQ数据读取
 *
 */
public class FlexQInputOperator extends InnerInputSourceOperator
{
    /**
     * 分组Id
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_METAQ_GROUPID)
    private String groupId;

    /**
     * OPERATOR_METAQ_PREFIX
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_METAQ_PREFIX)
    private String prefix;

    /**
     * Topic
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_METAQ_TOPIC)
    private String topic;

    /**
     * METAQ读取数据的zookeeper地址
     * 地址加端口，多个之间用逗号分隔
     * 比如：192.168.0.2:2181,192.168.0.3:2181
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_METAQ_ZOOKEEPERS)
    private String zookeepers;


    /**
     * <默认构造函数>
     */
    public FlexQInputOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }

    public String getZookeepers()
    {
        return zookeepers;
    }

    public void setZookeepers(String zookeepers)
    {
        this.zookeepers = zookeepers.trim();
    }

    public String getGroupId()
    {
        return groupId;
    }

    public void setGroupId(String groupId)
    {
        this.groupId = groupId.trim();
    }

    public String getTopic()
    {
        return topic;
    }

    public void setTopic(String topic)
    {
        this.topic = topic.trim();
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

}
