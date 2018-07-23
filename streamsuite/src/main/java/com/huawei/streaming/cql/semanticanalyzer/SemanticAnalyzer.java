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

import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;

/**
 * 语义分析接口
 * 
 * 语义分析发生在所有的CQL语句都完成了语法分析之后，
 * 语义分析是有顺序的，必须按照流使用的先后顺序进行分析
 * 
 * 语义fenix功能如下
 * 1：重新组织语义分析结果，比如表达式，函数之类
 * 2、检查各种数据类型是否一致。
 * 
 * 
 * 
 */
public interface SemanticAnalyzer
{
    /**
     * 初始化语义分析器
     */
    void init(List<Schema> schemas)
        throws SemanticAnalyzerException;
    
    /**
     * 语义分析
     */
    AnalyzeContext analyze()
        throws SemanticAnalyzerException;
}
