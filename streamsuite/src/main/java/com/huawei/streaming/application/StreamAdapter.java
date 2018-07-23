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

import com.huawei.streaming.operator.IRichOperator;

/**
 * 流处理算子适配接口
 * 依靠这个接口，将流处理的算子注入到具体的Storm算 Bolt中
 * 
 */
public interface StreamAdapter
{
    /**
     * 设置流处理算子
     */
    void setOperator(IRichOperator operator);
}
