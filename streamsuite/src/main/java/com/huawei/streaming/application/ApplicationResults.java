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
package com.huawei.streaming.application;

import java.util.List;

/**
 * 应用程序检查结果
 *
 */
public interface ApplicationResults
{
    /**
     * 获取列格式化字符串
     */
    String getFormatter();
    
    /**
     * 获取应用程序查询结果的标题头
     */
    String[] getResultHeader();
    
    /**
     * 获取查询结果
     * 查询结果的列数量必须和标题头的数组数量一致
     */
    List<String[]> getResults(String container);
    
}
