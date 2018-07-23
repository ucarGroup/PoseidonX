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
import com.huawei.streaming.cql.executor.operatorinfocreater.FunctionStreamInfoCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * 自定义算子
 *
 */
@XStreamAlias("FunctionStream")
@OperatorInfoCreatorAnnotation(FunctionStreamInfoCreator.class)
public class FunctionStreamOperator extends Operator
{
    /**
     * 算子所在类
     * 该类必须继承自FunctionStream、InputStream或者OutputStream
     * 如果继承自FunctionStream，则说明属于功能性算子
     * 如果继承自InputStream或者OutputStream，则说明属于输入输出算子
     * 该类必须包含一个无参构造函数，所有的配置参数保存在Config中。
     */
    private String operatorClass;
    
    private Schema inputSchema;
    
    private Schema outputSchema;
    
    /**
     * 数据分发方式
     * 如果为空，则随机分发
     */
    private String distributedColumnName;
    
    /**
     * <默认构造函数>
     *
     */
    public FunctionStreamOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public String getOperatorClass()
    {
        return operatorClass;
    }
    
    public void setOperatorClass(String operatorClass)
    {
        this.operatorClass = operatorClass;
    }

    public String getDistributedColumnName()
    {
        return distributedColumnName;
    }

    public void setDistributedColumnName(String distributedColumnName)
    {
        this.distributedColumnName = distributedColumnName;
    }

    public Schema getInputSchema()
    {
        return inputSchema;
    }

    public void setInputSchema(Schema inputSchema)
    {
        this.inputSchema = inputSchema;
    }

    public Schema getOutputSchema()
    {
        return outputSchema;
    }

    public void setOutputSchema(Schema outputSchema)
    {
        this.outputSchema = outputSchema;
    }
    
}
