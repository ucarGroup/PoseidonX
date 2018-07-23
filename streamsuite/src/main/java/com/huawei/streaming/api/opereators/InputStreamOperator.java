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

import com.huawei.streaming.cql.executor.operatorinfocreater.InputInfoCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * 输入算子
 * 
 * 定义了输入算子的统一规则，便于拓展
 * 所有的输入算子比如csv，kafka，tcp等都遵循此规则
 * 
 */
@OperatorInfoCreatorAnnotation(InputInfoCreator.class)
public class InputStreamOperator extends Operator
{
    /**
     * 反序列化类名称
     */
    @XStreamAlias("deserializerClass")
    private String deserializerClassName;
    
    /**
     * 数据读取的类名称
     * 一定是完整的类路径
     */
    @XStreamAlias("recordReader")
    private String recordReaderClassName;
    
    /**
     * <默认构造函数>
     */
    public InputStreamOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public String getRecordReaderClassName()
    {
        return recordReaderClassName;
    }
    
    public void setRecordReaderClassName(String recordReaderClassName)
    {
        this.recordReaderClassName = recordReaderClassName;
    }
    
    public String getDeserializerClassName()
    {
        return deserializerClassName;
    }
    
    public void setDeserializerClassName(String deserializerClassName)
    {
        this.deserializerClassName = deserializerClassName;
    }
    
}
