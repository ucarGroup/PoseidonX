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

package com.huawei.streaming.udfs;

import java.util.Map;

/**
 * 字符串裁剪函数
 * 
 * 该函数实现了两个功能：
 * substring(string|binary A, int start)
 * substring(string|binary A, int start, int len)
 * 
 */
@UDFAnnotation("substr")
public class SubString extends UDF
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 733782651952932209L;
    
    private static final int INDEXARRLEN = 2;
    
    private final int[] index;
    
    /**
     * <默认构造函数>
     */
    public SubString(Map<String, String> config)
    {
        super(config);
        index = new int[INDEXARRLEN];
    }
    
    /**
     * substring(string|binary A, int start, int len)
     */
    public String evaluate(String s, int pos, int len)
    {
        
        if (s == null)
        {
            return null;
        }
        
        if (len <= 0)
        {
            return "";
        }
        
        int[] idx = makeIndex(pos, len, s.length());
        if (idx == null)
        {
            return "";
        }
        
        return s.substring(idx[0], idx[1]);
    }
    
    /**
     * substring(string|binary A, int start) 
     */
    public String evaluate(String s, int pos)
    {
        return evaluate(s, pos, s.length());
    }
    
    private int[] makeIndex(int pos, int len, int inputLen)
    {
        if (Math.abs(pos) > inputLen)
        {
            return null;
        }
        
        int start, end;
        
        if (pos > 0)
        {
            start = pos - 1;
        }
        else if (pos < 0)
        {
            start = inputLen + pos;
        }
        else
        {
            start = 0;
        }
        
        if ((inputLen - start) < len)
        {
            end = inputLen;
        }
        else
        {
            end = start + len;
        }
        index[0] = start;
        index[1] = end;
        return index;
    }
    
}
