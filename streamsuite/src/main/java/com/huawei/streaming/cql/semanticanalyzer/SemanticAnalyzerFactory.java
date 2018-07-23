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

package com.huawei.streaming.cql.semanticanalyzer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 语义分析工厂方法
 * <p/>
 * 为什么不愿意放在parseContext接口中定义，是为了保持业务的隔离，
 * 两个接口做的不一样的事情，语法分析接口中突然冒出一个创建语义分析实例的接口，两个又没有多大的关联，不应该耦合在一起。
 * 放这里的不好就是当有新的语法添加的时候，这个类也要修改，如果不修改，编译也不会出错，所以很容易忘记修改。
 *
 */
public class SemanticAnalyzerFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(SemanticAnalyzerFactory.class);
    
    /**
     * 创建语义分析器
     *
     */
    public static SemanticAnalyzer createAnalyzer(ParseContext parseContext, List<Schema> schemas)
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer analyzer = parseContext.createAnalyzer();
        checkAnalyzerNull(parseContext, analyzer);
        analyzer.init(schemas);
        return analyzer;
    }
    
    private static void checkAnalyzerNull(ParseContext parseContext, SemanticAnalyzer analyzer)
        throws SemanticAnalyzerException
    {
        if (analyzer == null)
        {
            LOG.error("Can not find analyzer for parse context '{}'.", parseContext.getClass().getName());
            SemanticAnalyzerException exception = new SemanticAnalyzerException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
    }
}
