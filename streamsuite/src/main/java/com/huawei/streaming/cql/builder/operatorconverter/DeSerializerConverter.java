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
import com.huawei.streaming.api.opereators.InputStreamOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.serdes.SerDeAPI;
import com.huawei.streaming.api.opereators.serdes.UserDefinedSerDeAPI;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * 反序列化接口转换
 *
 */
public class DeSerializerConverter implements Converter
{
    private static final Logger LOG = LoggerFactory.getLogger(DeSerializerConverter.class);
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(Operator op)
    {
        return op instanceof InputStreamOperator;
    }
    
    /**
     * 反序列化类转换
     *
     */
    public SerDeAPI convert(InputStreamOperator op)
        throws ApplicationBuildException
    {
        String rss = InputOutputOperatorMapping.getAPIOperatorByPlatform(op.getDeserializerClassName());
        if (rss == null)
        {
            return convertUserDeser(op);
        }
        return convertSystemDeSer(op, rss);
    }
    
    private SerDeAPI convertUserDeser(InputStreamOperator op)
        throws ApplicationBuildException
    {
        UserDefinedSerDeAPI userDeser = new UserDefinedSerDeAPI();
        userDeser.setConfig(new TreeMap< String, String >());
        for (Entry<String, String> et : op.getArgs().entrySet())
        {
            userDeser.getConfig().put(et.getKey(), et.getValue());
        }
        setDeSerializerClass(op, userDeser);
        removeUserRepeatConfig(op.getArgs(), userDeser);
        return userDeser;
    }
    
    private void removeUserRepeatConfig(Map<String, String> config, UserDefinedSerDeAPI userDeser)
    {
        Map<String, String> opConf = userDeser.getConfig();
        for (Entry<String, String> et : opConf.entrySet())
        {
            config.remove(et.getKey());
        }
    }
    
    @SuppressWarnings("unchecked")
    private void setDeSerializerClass(InputStreamOperator op, UserDefinedSerDeAPI userDeser)
        throws ApplicationBuildException
    {
        String deSerClassName = op.getDeserializerClassName();
        
        try
        {
            Class< ? extends StreamSerDe> getDescClass =
                (Class< ? extends StreamSerDe>)Class.forName(deSerClassName, true, CQLUtils.getClassLoader());
            userDeser.setSerDeClazz(getDescClass);
        }
        catch (ClassNotFoundException e)
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, deSerClassName);
            LOG.error("Failed to set DeSerializer class.", exception);
            throw exception;
        }
    }
    
    private SerDeAPI convertSystemDeSer(InputStreamOperator op, String rs)
        throws ApplicationBuildException
    {
        SerDeAPI deserApi = createSerDeAPIInstance(rs);
        AnnotationUtils.setConfigToObject(deserApi, op.getArgs());
        removeSystemRepeatConfig(op.getArgs(), deserApi);
        return deserApi;
    }
    
    private SerDeAPI createSerDeAPIInstance(String serdeClass)
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
            LOG.error("Failed to create SerDe api instance.", exception);
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
    
}
