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

package com.huawei.streaming.application;

import java.lang.reflect.Constructor;

import com.huawei.streaming.cql.DriverContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * 创建应用程序的工厂类
 * 完成Application和底层Storm平台的解耦
 *
 */
public class ApplicationFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationFactory.class);
    
    /**
     * 创建应用程序
     *
     */
    public static Application createApplication(DriverContext driverContext, StreamingConfig conf, String name)
        throws StreamingException
    {
        
        String appClassName = (String)conf.get(StreamingConfig.STREAMING_ADAPTOR_APPLICATION);
        
        try
        {
            Class< ? > appClass = Class.forName(appClassName);
            Constructor< ? > constructor = appClass.getConstructor(StreamingConfig.class, String.class,DriverContext.class);
            return (Application)constructor.newInstance(conf, name,driverContext);
        }
        catch (Exception e)
        {
            if (e instanceof StreamingException)
            {
                throw (StreamingException)e;
            }
            
            StreamingException exception = new StreamingException(e, ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR.getFullMessage(), e);
            throw exception;
        }
    }
}
