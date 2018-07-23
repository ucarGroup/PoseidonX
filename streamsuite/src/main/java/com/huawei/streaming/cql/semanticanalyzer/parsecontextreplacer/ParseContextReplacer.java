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

package com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer;

import com.huawei.streaming.cql.semanticanalyzer.parser.context.BaseExpressionParseContext;

/**
 * 表达式替换接口
 * 将一个表达式替换为另外一个 ，通常适用于having等子句
 * 
 */
public interface ParseContextReplacer
{
    /**
     * 检查是否有子节点内容可以进行替换
     */
    boolean isChildsReplaceable(BaseExpressionParseContext parseContext);
    
    /**
     * 创建要替换的表达式
     */
    BaseExpressionParseContext createReplaceParseContext();
    
}
