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

package com.huawei.streaming.cql.builder.operatorconverter;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.AnnotationUtils;
import com.huawei.streaming.api.opereators.InnerInputSourceOperator;
import com.huawei.streaming.api.opereators.InputStreamOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 输入算子转换
 * <p/>
 * 具体步骤：
 * 1、获取底层对应的recordreader class，比如kafkaSourceOp
 * 2、如果底层获取没有对应的recordreader，那么就按照自定义输入来处理
 * 3、创建底层recordreader实例
 * 4、将input中的配置属性通过反射设置到recordreader中，并移除已经使用过的配置属性
 * 5、使用反序列化转换器进行转换，并移除相应配置属性
 *
 */
public class InputConverter implements OperatorConverter
{
    private static final Logger LOG = LoggerFactory.getLogger(InputConverter.class);
    
    private DeSerializerConverter deserConverter = new DeSerializerConverter();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(Operator op)
    {
        return op instanceof InputStreamOperator;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Operator convert(Operator op)
        throws ApplicationBuildException
    {
        InputStreamOperator inputop = (InputStreamOperator)op;
        String rss = InputOutputOperatorMapping.getAPIOperatorByPlatform(inputop.getRecordReaderClassName());
        if (null == rss)
        {
            return op;
        }
        return convertToBasicInputSourceOperator(op, inputop, rss);
    }
    
    private InnerInputSourceOperator convertToBasicInputSourceOperator(Operator op, InputStreamOperator inputop,
        String rs)
        throws ApplicationBuildException
    {
        InnerInputSourceOperator innerInputSourceOperator = createInputSourceOperatorInstance(op, rs);
        AnnotationUtils.setConfigToObject(innerInputSourceOperator, op.getArgs());
        removeRepeatConfig(op.getArgs(), innerInputSourceOperator);
        innerInputSourceOperator.setDeserializer(deserConverter.convert(inputop));
        if(op.getArgs() != null && op.getArgs().size() != 0)
        {
            innerInputSourceOperator.setArgs(op.getArgs());
        }

        return innerInputSourceOperator;
    }
    
    private InnerInputSourceOperator createInputSourceOperatorInstance(Operator op,
        String innerInputSourceOperatorClassName)
        throws ApplicationBuildException
    {
        try
        {
            Constructor< ? > constructor =
                Class.forName(innerInputSourceOperatorClassName, true, CQLUtils.getClassLoader())
                    .getConstructor(String.class, int.class);
            return (InnerInputSourceOperator)constructor.newInstance(op.getId(), op.getParallelNumber());
        }
        catch (ReflectiveOperationException e)
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS,
                    innerInputSourceOperatorClassName);
            LOG.error("Failed to create input source operator instance.", exception);
            throw exception;
        }
    }
    
    private void removeRepeatConfig(Map<String, String> config, InnerInputSourceOperator ob)
        throws ApplicationBuildException
    {
        Map<String, String> opConf = AnnotationUtils.getAnnotationsToConfig(ob);
        for (Entry<String, String> et : opConf.entrySet())
        {
            config.remove(et.getKey());
        }
    }
    
}
