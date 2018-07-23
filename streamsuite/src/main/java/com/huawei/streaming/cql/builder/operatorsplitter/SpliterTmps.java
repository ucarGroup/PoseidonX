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

package com.huawei.streaming.cql.builder.operatorsplitter;

import java.text.DecimalFormat;

/**
 * spliter中的一些临时变量存放
 * 
 */
public class SpliterTmps
{
    private static final char DEFAULT_REPLACE_STR_PREFIX = 127;
    
    /**
     * 获取下一索引
     */
    public static String formatIndex(Integer index)
    {
        return DEFAULT_REPLACE_STR_PREFIX + getNextNumber(index);
    }
    
    private static String getNextNumber(int nowNumber)
    {
        DecimalFormat formatter = new DecimalFormat("000");
        return formatter.format(nowNumber);
        
    }
}
