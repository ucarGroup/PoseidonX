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
package com.huawei.streaming.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * 安全实例创建工厂
 *
 */
public class SecurityFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(SecurityFactory.class);
    
    private static final String NONE_SECURITY = "NONE";
    
    private static final String KERBEROS_TYPE = "KERBEROS";
    
    /**
     * 创建Streaming安全实例
     */
    public static StreamingSecurity createSecurity(StreamingConfig config)
        throws StreamingException
    {
        if (config == null)
        {
            LOG.error("Config is null.");
            throw new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
        }
        
        SecurityType securityType = getSecurityType(config);
        switch (securityType)
        {
            case KERBEROS:
                return new KerberosSecurity(config);
            default:
                return new NoneSecurity();
        }
    }
    
    /**
     * 获取安全类型
     */
    public static SecurityType getSecurityType(StreamingConfig config)
        throws StreamingException
    {
        Object value = config.get(StreamingConfig.STREAMING_SECURITY_AUTHENTICATION);
        if (value == null || Strings.isNullOrEmpty(value.toString()))
        {
            StreamingException exception =
                new StreamingException(ErrorCode.CONFIG_NOT_FOUND, StreamingConfig.STREAMING_SECURITY_AUTHENTICATION);
            LOG.error("Can't find security authentication in config.", exception);
            throw exception;
        }
        
        return parseSecurityType(value);
    }
    
    private static SecurityType parseSecurityType(Object value)
        throws StreamingException
    {
        if (value.toString().equalsIgnoreCase(NONE_SECURITY))
        {
            return SecurityType.NONE;
        }
        
        if (value.toString().equalsIgnoreCase(KERBEROS_TYPE))
        {
            return SecurityType.KERBEROS;
        }
        
        StreamingException exception = new StreamingException(ErrorCode.SECURITY_UNSUPPORTED_TYPE, value.toString());
        LOG.error("Wrong security type.", exception);
        throw exception;
    }
}
