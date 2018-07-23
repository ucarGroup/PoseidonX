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

import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;

/**
 * 通用的语句split接口
 * 
 */
public interface Splitter
{
    /**
     * 验证能否进行解析
     */
    boolean validate(AnalyzeContext parseContext)
        throws ApplicationBuildException;
    
    /**
     * 将解析结果拆分成多个算子
     */
    SplitContext split(AnalyzeContext parseContext)
        throws ApplicationBuildException;
}
