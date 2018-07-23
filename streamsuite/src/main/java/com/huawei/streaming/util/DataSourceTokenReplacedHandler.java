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

package com.huawei.streaming.util;

import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * 将 字符串中满足匹配的内容进行替换
 *
 */
public class DataSourceTokenReplacedHandler implements TokenHandler, Serializable
{
    private static final long serialVersionUID = -7832147018242840870L;
    
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceTokenReplacedHandler.class);
    
    private final Map<String, Object> cqlExpressionValues;
    
    /**
     * <默认构造函数>
     *
     */
    public DataSourceTokenReplacedHandler(Map<String, Object> expValues)
    {
        this.cqlExpressionValues = expValues;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String handleToken(String content)
        throws StreamingException
    {
        if (content == null || content.trim().length() == 0)
        {
            return "";
        }
        
        if (cqlExpressionValues.containsKey(content))
        {
            return cqlExpressionValues.get(content).toString();
        }
        
        StreamingException exception =
            new StreamingException(ErrorCode.SEMANTICANALYZE_DATASOURDE_NO_ARGUMENT, content);
        LOG.error(ErrorCode.SEMANTICANALYZE_DATASOURDE_NO_ARGUMENT.getFullMessage(content), exception);
        throw exception;
    }
}
