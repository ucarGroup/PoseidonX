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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext;

import java.util.List;

import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * 语义分析结果通用接口
 */
public abstract class AnalyzeContext
{
    /**
     * 设置语法解析结果
     */
    public abstract void setParseContext(ParseContext parseContext);
    
    /**
     * 验证语法分析结果是否正确，比如数据类型是否匹配等
     */
    public abstract void validateParseContext()
        throws SemanticAnalyzerException;
    
    /**
     * 从解析结果中获取schema信息
     * 仅限于create stream或者selec他这样的语句可以
     * 其他的类似limit之类就咩有schema信息
     */
    public abstract List<Schema> getCreatedSchemas();
    
    /**
     * 将解析内容还原为表达式
     * 
     * 使用toString接口是因为guava join的方法需要用到toString方法进行list的拼接
     * 也正是因为这个toString方法，analzyeContext才变成了抽象类，没有使用接口。
     * 
     */
    public abstract String toString();
    
}
