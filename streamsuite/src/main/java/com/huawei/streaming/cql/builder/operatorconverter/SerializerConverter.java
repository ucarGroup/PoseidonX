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
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.AnnotationUtils;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OutputStreamOperator;
import com.huawei.streaming.api.opereators.serdes.SerDeAPI;
import com.huawei.streaming.api.opereators.serdes.UserDefinedSerDeAPI;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * 序列化接口转换
 *
 */
public class SerializerConverter implements Converter
{
    
    private static final Logger LOG = LoggerFactory.getLogger(DeSerializerConverter.class);
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(Operator op)
    {
        return op instanceof OutputStreamOperator;
    }
    
    /**
     * 反序列化类转换
     *
     */
    public SerDeAPI convert(OutputStreamOperator op)
        throws ApplicationBuildException
    {
        String rss = InputOutputOperatorMapping.getAPIOperatorByPlatform(op.getSerializerClassName());
        if (rss == null)
        {
            return convertUserDeser(op);
        }
        return convertSystemDeser(op, rss);
    }
    
    private SerDeAPI convertSystemDeser(OutputStreamOperator op, String rs)
        throws ApplicationBuildException
    {
        SerDeAPI biop = createSerDeInstance(rs);
        AnnotationUtils.setConfigToObject(biop, op.getArgs());
        removeSystemRepeatConfig(op.getArgs(), biop);
        return biop;
    }
    
    private SerDeAPI createSerDeInstance(String serdeClass)
        throws ApplicationBuildException
    {
        try
        {
            Constructor< ? > constructor = Class.forName(serdeClass, true, CQLUtils.getClassLoader()).getConstructor();
            return (SerDeAPI)constructor.newInstance();
        }
        catch (ReflectiveOperationException e)
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, serdeClass);
            LOG.error("Failed to create SerDe instance.", exception);
            throw exception;
        }
    }
    
    private void removeSystemRepeatConfig(Map<String, String> config, SerDeAPI ob)
        throws ApplicationBuildException
    {
        Map<String, String> opConf = AnnotationUtils.getAnnotationsToConfig(ob);
        for (Entry<String, String> et : opConf.entrySet())
        {
            config.remove(et.getKey());
        }
    }
    
    private SerDeAPI convertUserDeser(OutputStreamOperator op)
        throws ApplicationBuildException
    {
        UserDefinedSerDeAPI userDeser = new UserDefinedSerDeAPI();
        userDeser.setConfig(new TreeMap< String, String >());
        for (Entry<String, String> et : op.getArgs().entrySet())
        {
            userDeser.getConfig().put(et.getKey(), et.getValue());
        }
        
        setUserDefinedSerdeClasss(op, userDeser);
        removeUserRepeatConfig(op.getArgs(), userDeser);
        
        return userDeser;
    }
    
    @SuppressWarnings("unchecked")
    private void setUserDefinedSerdeClasss(OutputStreamOperator op, UserDefinedSerDeAPI userDeser)
        throws ApplicationBuildException
    {
        try
        {
            userDeser.setSerDeClazz((Class< ? extends StreamSerDe>)Class.forName(op.getSerializerClassName(),
                true,
                CQLUtils.getClassLoader()));
        }
        catch (ClassNotFoundException e)
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, op.getSerializerClassName());
            LOG.error("Failed to create user defied Serde instance.", exception);
            throw exception;
        }
        
    }
    
    private void removeUserRepeatConfig(Map<String, String> config, UserDefinedSerDeAPI userDeser)
        throws ApplicationBuildException
    {
        Map<String, String> opConf = userDeser.getConfig();
        for (Entry<String, String> et : opConf.entrySet())
        {
            config.remove(et.getKey());
        }
    }
}
