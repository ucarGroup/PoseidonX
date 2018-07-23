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
import com.huawei.streaming.api.opereators.InnerOutputSourceOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OutputStreamOperator;
import com.huawei.streaming.api.opereators.serdes.SerDeAPI;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 输出算子转换
 * <p/>
 * 具体步骤：
 * 1、获取底层对应的recordWriter class，比如kafkaFunctionOp
 * 2、如果底层获取没有对应的recordWriter，那么就按照自定义输出来处理
 * 3、创建底层recordWriter实例
 * 4、将output中的配置属性通过反射设置到recordWriter中，并移除已经使用过的配置属性
 * 5、使用序列化转换器进行转换，并移除相应配置属性
 *
 */
public class OutputConverter implements OperatorConverter
{
    private static final Logger LOG = LoggerFactory.getLogger(OutputConverter.class);
    
    private SerializerConverter serConverter = new SerializerConverter();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(Operator op)
    {
        return op instanceof OutputStreamOperator;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Operator convert(Operator op)
        throws ApplicationBuildException
    {
        OutputStreamOperator inputop = (OutputStreamOperator)op;
        String rss = InputOutputOperatorMapping.getAPIOperatorByPlatform(inputop.getRecordWriterClassName());
        
        if (null == rss)
        {
            return op;
        }
        
        return convertBasicOutputSourceOperator(op, inputop, rss);
    }
    
    private InnerOutputSourceOperator convertBasicOutputSourceOperator(Operator op, OutputStreamOperator outputop,
        String rs)
        throws ApplicationBuildException
    {
        InnerOutputSourceOperator biop = createInnerOutputOperatorInstance(op, rs);
        AnnotationUtils.setConfigToObject(biop, op.getArgs());
        removeRepeatConfig(op.getArgs(), biop);
        if(op.getArgs() != null && op.getArgs().size() != 0)
        {
            outputop.setArgs(op.getArgs());
        }

        SerDeAPI deserapi = serConverter.convert(outputop);
        biop.setSerializer(deserapi);
        return biop;
    }
    
    private InnerOutputSourceOperator createInnerOutputOperatorInstance(Operator op, String operatorClassName)
        throws ApplicationBuildException
    {
        try
        {
            Class< ? > operatorClass = Class.forName(operatorClassName, true, CQLUtils.getClassLoader());
            Constructor< ? > constructor = operatorClass.getConstructor(String.class, int.class);
            return (InnerOutputSourceOperator)constructor.newInstance(op.getId(), op.getParallelNumber());
        }
        catch (ReflectiveOperationException e)
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, operatorClassName);
            LOG.error("Failed to create inner output operator instance.", exception);
            throw exception;
        }
    }
    
    private void removeRepeatConfig(Map<String, String> config, InnerOutputSourceOperator ob)
        throws ApplicationBuildException
    {
        Map<String, String> opConf = AnnotationUtils.getAnnotationsToConfig(ob);
        for (Entry<String, String> et : opConf.entrySet())
        {
            config.remove(et.getKey());
        }
    }
    
}
