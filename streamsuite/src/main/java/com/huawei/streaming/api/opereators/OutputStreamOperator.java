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

import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;
import com.huawei.streaming.cql.executor.operatorinfocreater.OutputInfoCreator;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * 输出算子
 * 
 * 定义了输出算子的统一规则，便于拓展
 * 所有的输出算子比如csv，kafka，tcp等都遵循此规则
 */
@OperatorInfoCreatorAnnotation(OutputInfoCreator.class)
public class OutputStreamOperator extends Operator
{
    /**
     * 数据序列化的类名
     */
    @XStreamAlias("serializerClass")
    private String serializerClassName;
    
    /**
     * 数据写入用到的类名称
     * 一定是完整的类路径
     */
    @XStreamAlias("recordWriter")
    private String recordWriterClassName;
    
    /**
     * <默认构造函数>
     */
    public OutputStreamOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public String getSerializerClassName()
    {
        return serializerClassName;
    }
    
    public void setSerializerClassName(String serializerClassName)
    {
        this.serializerClassName = serializerClassName;
    }
    
    public String getRecordWriterClassName()
    {
        return recordWriterClassName;
    }
    
    public void setRecordWriterClassName(String recordWriterClassName)
    {
        this.recordWriterClassName = recordWriterClassName;
    }
    
}
