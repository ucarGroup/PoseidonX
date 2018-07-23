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
 * 字符串拼接的函数
 * 
 * 将多个字符串拼接成一个
 * 
 * 多个字符串中，如果有一个为空，就都返回空
 * 
 */
@UDFAnnotation("concat")
public class StringConcat extends UDF
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -6107600743890077789L;

    /**
     * <默认构造函数>
     */
    public StringConcat(Map<String, String> config)
    {
        super(config);
    }
    
    /**
     * 字符串拼接
     */
    public String evaluate(String arg1, String arg2)
    {
        return evaluate(new String[] {arg1, arg2});
    }
    
    /**
     * 字符串拼接
     * 
     */
    public String evaluate(String arg1, String arg2, String arg3)
    {
        return evaluate(new String[] {arg1, arg2, arg3});
    }
    
    /**
     * 字符串拼接
     * 
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4});
    }
    
    /**
     * 字符串拼接
     * 
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5});
    }
    
    /**
     * 字符串拼接
     * 
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5, String arg6)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5, arg6});
    }
    
    /**
     * 字符串拼接
     * 
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5, String arg6, String arg7)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5, arg6, arg7});
    }

    /**
     * 字符串拼接
     *
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5, String arg6, String arg7, String arg8)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5, arg6, arg7,arg8});
    }

    /**
     * 字符串拼接
     *
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5, String arg6, String arg7, String arg8, String arg9)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5, arg6, arg7,arg8,arg9});
    }

    /**
     * 字符串拼接
     *
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5, String arg6, String arg7, String arg8, String arg9, String arg10)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5, arg6, arg7,arg8,arg9, arg10});
    }

    /**
     * 字符串拼接
     *
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5, String arg6, String arg7, String arg8, String arg9, String arg10, String arg11)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5, arg6, arg7,arg8,arg9, arg10,arg11});
    }

    /**
     * 字符串拼接
     *
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5, String arg6, String arg7, String arg8, String arg9, String arg10, String arg11, String arg12)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5, arg6, arg7,arg8,arg9, arg10,arg11,arg12});
    }

    /**
     * 字符串拼接
     *
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5, String arg6, String arg7, String arg8, String arg9, String arg10, String arg11, String arg12, String arg13)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5, arg6, arg7,arg8,arg9, arg10,arg11,arg12,arg13});
    }


    /**
     * 字符串拼接
     *
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5, String arg6, String arg7, String arg8, String arg9, String arg10, String arg11, String arg12, String arg13, String arg14)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5, arg6, arg7,arg8,arg9, arg10,arg11,arg12,arg13,arg14});
    }


    /**
     * 字符串拼接
     *
     */
    public String evaluate(String arg1, String arg2, String arg3, String arg4, String arg5, String arg6, String arg7, String arg8, String arg9, String arg10, String arg11, String arg12, String arg13, String arg14, String arg15)
    {
        return evaluate(new String[] {arg1, arg2, arg3, arg4, arg5, arg6, arg7,arg8,arg9, arg10,arg11,arg12,arg13,arg14,arg15});
    }
    
    /**
     * 字符串拼接
     */
    private String evaluate(String[] arguments)
    {
        StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < arguments.length; idx++)
        {
            String val = arguments[idx];
            if (val == null)
            {
                return null;
            }
            sb.append(val);
        }
        return sb.toString();
    }
}
