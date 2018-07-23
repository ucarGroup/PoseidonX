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

import com.huawei.streaming.exception.StreamingException;

/**
 * 解析字符串中的特殊配对字符串
 * 
 * 但是不允许openToken中套openToken
 * 
 */
public class GenericTokenParser implements Serializable
{
    private static final long serialVersionUID = -3675580900972980044L;
    
    private final String openToken;
    
    private final String closeToken;
    
    private final TokenHandler handler;
    
    /**
     * <默认构造函数>
     */
    public GenericTokenParser(String openToken, String closeToken, TokenHandler handler)
    {
        this.openToken = openToken;
        this.closeToken = closeToken;
        this.handler = handler;
    }
    
    /**
     * 解析字符串>
     */
    public String parse(String text)
        throws StreamingException
    {
        StringBuilder builder = new StringBuilder();
        if (text != null && text.length() > 0)
        {
            char[] src = text.toCharArray();
            int offset = 0;
            int start = text.indexOf(openToken, offset);
            while (start > -1)
            {
                if (start > 0 && src[start - 1] == '\\')
                {
                    builder.append(src, offset, start - 1).append(openToken);
                    offset = start + openToken.length();
                }
                else
                {
                    int end = text.indexOf(closeToken, start);
                    if (end == -1)
                    {
                        builder.append(src, offset, src.length - offset);
                        offset = src.length;
                    }
                    else
                    {
                        builder.append(src, offset, start - offset);
                        offset = start + openToken.length();
                        String content = new String(src, offset, end - offset);
                        builder.append(handler.handleToken(content));
                        offset = end + closeToken.length();
                    }
                }
                start = text.indexOf(openToken, offset);
            }
            if (offset < src.length)
            {
                builder.append(src, offset, src.length - offset);
            }
        }
        return builder.toString();
    }
    
}
