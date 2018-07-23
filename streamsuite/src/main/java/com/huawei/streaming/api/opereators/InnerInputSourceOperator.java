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

import com.huawei.streaming.api.opereators.serdes.SerDeAPI;
import com.huawei.streaming.cql.executor.operatorinfocreater.InputInfoCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;

/**
 * 基本的输入算子
 * 
 */
@OperatorInfoCreatorAnnotation(InputInfoCreator.class)
public class InnerInputSourceOperator extends Operator
{
    /**
     * 反序列化类名称
     * 这个不是参数，到时候会直接解析出来通过接口设置到sourceOperator中
     */
    private SerDeAPI deserializer;
    
    /**
     * <默认构造函数>
     * 
     */
    public InnerInputSourceOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public SerDeAPI getDeserializer()
    {
        return deserializer;
    }
    
    public void setDeserializer(SerDeAPI deserializer)
    {
        this.deserializer = deserializer;
    }
    
}
