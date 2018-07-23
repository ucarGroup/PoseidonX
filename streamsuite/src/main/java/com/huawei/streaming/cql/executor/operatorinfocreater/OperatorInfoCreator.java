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

package com.huawei.streaming.cql.executor.operatorinfocreater;

import java.util.Map;

import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.AbsOperator;

/**
 * 创建每个算子的实例
 * 
 */
public interface OperatorInfoCreator
{
    /**
     * 创建实例
     */
    AbsOperator createInstance(Application vapp, Operator operator, EventTypeMng streamschema,
                               Map<String, String> systemConfig)
        throws StreamingException;
}
