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

package com.huawei.streaming.cql.hooks;

/**
 * 执行器前后的钩子
 * 
 * 一个在数据反序列化之后，数据检查之前
 * 一个发生在应用程序提交之前，应用程序检查之前
 * 
 */
public interface ExecutorHook
{
    /**
     * 物理执行计划解析之后，数据检查之前执行的钩子
     */
    void preExecute(com.huawei.streaming.api.Application physicPlanApplication);
    
    /**
     * 应用程序提交之前执行的钩子
     */
    void preSubmit(com.huawei.streaming.application.Application submitApplication);
    
}
