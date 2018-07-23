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

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExplainStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.GetStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ShowApplicationsContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ShowFunctionsContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SubmitApplicationContext;

/**
 * CQL的一些公共方法
 * 
 */
public class CQLUtils
{
    /**
     * 不会对cql产生什么影响的一些命令集合
     */
    private static Set<Class< ? extends ParseContext>> nonChangeableCommonds = Sets.newHashSet();
    
    static
    {
        nonChangeableCommonds.add(ShowFunctionsContext.class);
        nonChangeableCommonds.add(ShowApplicationsContext.class);
        nonChangeableCommonds.add(GetStatementContext.class);
        nonChangeableCommonds.add(ExplainStatementContext.class);
        nonChangeableCommonds.add(SubmitApplicationContext.class);
    }
    
    /**
     * 从文件中读取cql语句
     */
    public static List<String> readCQLsFromFile(String file) throws CQLException
    {
        CQLFileReader reader = new CQLFileReader();
        reader.readCQLs(file);
        return reader.getResult();
    }
    
    /**
     * 检查该语法是否会对查询结果造成影响
     * 如果造成影响，返回 true
     */
    public static boolean isChangeableCommond(ParseContext parseContext)
    {
        return !nonChangeableCommonds.contains(parseContext.getClass());
    }
    
    /**
     * 移除字符串前后的‘号
     */
    public static String cqlStringLiteralTrim(String str)
    {
        if (str == null)
        {
            return null;
        }
        
        String s = str;
        if (s.startsWith("'") || s.startsWith("\""))
        {
            s = s.substring(1, s.length());
        }
        if (s.endsWith("'") || s.endsWith("\""))
        {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }
    
    /**
     * 获取当前线程的ClassLoader，如果不存在，就返回appclassloader
     */
    public static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null)
        {
            classLoader = CQLUtils.class.getClassLoader();
        }
        return classLoader;
    }
}
