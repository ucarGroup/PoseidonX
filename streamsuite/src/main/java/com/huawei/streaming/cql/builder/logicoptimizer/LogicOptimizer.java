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

package com.huawei.streaming.cql.builder.logicoptimizer;

import java.util.List;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;

/**
 * 逻辑优化器
 * 
 * 包含功能：
 * 1、SQL语句的重写，比如将where中的聚合filter调整到having中等等
 * 2、count(a+b),count(*),count(a) 的优化，全部改成count(1)
 * 3、Join的调整，将不等值Join改为Innerjoin
 * 4、将where条件中的等值表达式提升到On上面去。
 * 
 */
public class LogicOptimizer implements Transform
{
    /**
     * 初始化
     */
    public void init(StreamingConfig conf)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<AnalyzeContext> transform(List<AnalyzeContext> parseContexts)
    {
        return parseContexts;
    }
}
