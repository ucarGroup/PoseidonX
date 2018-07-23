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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * 数据源关键词替换
 * 
 */
public class DataSourceTokenCollectorHandler implements TokenHandler, Serializable
{
    private static final long serialVersionUID = -7832147018242840870L;
    
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceTokenCollectorHandler.class);
    
    private Set<String> contents;
    
    private final String openToken;
    
    /**
     * <默认构造函数>
     */
    public DataSourceTokenCollectorHandler(String openToken, String closeToken)
    {
        this.openToken = openToken;
        contents = Sets.newHashSet();
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
            return null;
        }
        
        if (content.contains(openToken))
        {
            StreamingException exception =
                new StreamingException(ErrorCode.SEMANTICANALYZE_DATASOURDE_NO_ARGUMENT, content);
            LOG.error(ErrorCode.SEMANTICANALYZE_DATASOURDE_NO_ARGUMENT.getFullMessage(content), exception);
            throw exception;
        }

        contents.add(content.trim());
        return content;
    }
    
    public Set<String> getContents()
    {
        return contents;
    }
}
