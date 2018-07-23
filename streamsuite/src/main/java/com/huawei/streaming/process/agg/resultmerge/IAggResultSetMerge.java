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

package com.huawei.streaming.process.agg.resultmerge;

import com.huawei.streaming.process.join.IJoinSetProcessor;

/**
 * 统一的aggregate ResoutMerge 接口
 * 
 * 原来需要两个接口，Join依赖一个接口，AggregateService又依赖于另一个接口
 * 为了便于CQL进行统一处理，统一依赖于一个接口
 * 
 */
public interface IAggResultSetMerge extends IJoinSetProcessor, IResultSetMerge
{
    
}
