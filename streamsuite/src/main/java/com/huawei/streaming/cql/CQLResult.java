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

package com.huawei.streaming.cql;

import java.util.Arrays;
import java.util.List;

/**
 * 所有CQL语句运行的结果
 * 
 */
public class CQLResult
{
    /**
     * 返回结果的消息头
     * 即每列的列名称
     */
    private String[] heads;
    
    /**
     * 字符串格式化
     */
    private String formatter;
    
    /**
     * 返回的结果集
     * list中的字符串数组大小和heads大小一样
     */
    private List<String[]> results;
    
    public String[] getHeads()
    {
        return heads == null ? new String[] {} : (String[])heads.clone();
    }
    
    public void setHeads(String[] heads)
    {
        this.heads = Arrays.copyOf(heads, heads.length);
    }
    
    public List<String[]> getResults()
    {
        return results;
    }
    
    public void setResults(List<String[]> results)
    {
        this.results = results;
    }
    
    public String getFormatter()
    {
        return formatter;
    }
    
    public void setFormatter(String formatter)
    {
        this.formatter = formatter;
    }
    
}
