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

import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.DistributeType;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

/**
 * 各种Operator的连接关系
 * 定义了从一个operator到另外一个operator的连接
 * 
 * 
 * 2013.07.23讨论结果
 * 1、同一个spout，输出流名称必须相同
 * 2、同一个算子输出流名称可以一样
 * 3、同一个算子，输入流名称不允许重复
 * 4、不同的算子，流名称必然不一样
 * 5、同一个算子，分发特征可以不一样，但是schema名称和流名称必须一样
 * 
 */
public class OperatorTransition
{
    /**
     * 当前连接的id, 每个连接的id都不相同。
     * 每个id之间用id来进行比较
     * id不能为空，CQL客户端和IDE客户端都会用到。
     */
    @XStreamAsAttribute
    @XStreamOmitField
    private String id;
    
    /**
     * 流名称
     */
    @XStreamAsAttribute
    @XStreamAlias("name")
    private String streamName;
    
    /**
     * 发起连接的Operator id
     */
    @XStreamAsAttribute
    @XStreamAlias("from")
    private String fromOperatorId;
    
    /**
     * 接收连接的Operator id
     */
    @XStreamAsAttribute
    @XStreamAlias("to")
    private String toOperatorId;
    
    /**
     * 数据获取类型
     * 仅仅在非sourceOperator中存在。
     */
    @XStreamAsAttribute
    private DistributeType distributedType;
    
    /**
     * 数据分发字段
     * 仅在distributedType为field的时候生效
     */
    @XStreamAsAttribute
    private String distributedFields;
    
    /**
     * 流上进行数据传输的时候的schema名称
     */
    @XStreamAsAttribute
    private String schemaName;
    
    /**
     * <默认构造函数>
     */
    public OperatorTransition(String streamname, Operator fromOperator, Operator toOperator,
        DistributeType distributedType, String distributedFiles, Schema schema)
    {
        super();
        this.id = streamname;
        this.streamName = streamname;
        this.fromOperatorId = fromOperator.getId();
        this.toOperatorId = toOperator.getId();
        this.distributedType = distributedType;
        this.distributedFields = distributedFiles;
        this.schemaName = schema.getId();
    }
    
    public String getId()
    {
        return id;
    }
    
    public void setId(String id)
    {
        this.id = id;
    }
    
    public String getFromOperatorId()
    {
        return fromOperatorId;
    }
    
    public void setFromOperatorId(String fromOperatorId)
    {
        this.fromOperatorId = fromOperatorId;
    }
    
    public String getToOperatorId()
    {
        return toOperatorId;
    }
    
    public void setToOperatorId(String toOperatorId)
    {
        this.toOperatorId = toOperatorId;
    }
    
    public DistributeType getDistributedType()
    {
        return distributedType;
    }
    
    public void setDistributedType(DistributeType distributedType)
    {
        this.distributedType = distributedType;
    }
    
    public String getSchemaName()
    {
        return schemaName;
    }
    
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }
    
    public String getStreamName()
    {
        return streamName;
    }
    
    public void setStreamName(String streamName)
    {
        this.streamName = streamName;
    }
    
    public String getDistributedFields()
    {
        return distributedFields;
    }
    
    public void setDistributedFields(String distributedFields)
    {
        this.distributedFields = distributedFields;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "OperatorTransition [streamName=" + streamName + ", fromOperatorId=" + fromOperatorId
            + ", toOperatorId=" + toOperatorId + ", distributedType=" + distributedType + ", distributedFields="
            + distributedFields + ", schemaName=" + schemaName + "]";
    }
    
}
