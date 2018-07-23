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

package com.huawei.streaming.api.opereators;

import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;
import com.huawei.streaming.cql.executor.operatorinfocreater.UnionInfoCreator;

/**
 * Union算子
 * 
 * Union算子中不支持select，也不支持filter，更不支持udf函数
 * 仅仅起到多流汇聚的作用，而且要求多个流的schema中，必须包含输出schema的那一列
 * 
 */
@OperatorInfoCreatorAnnotation(UnionInfoCreator.class)
public class UnionOperator extends InnerFunctionOperator
{
    /**
     * <默认构造函数>
     * 
     */
    public UnionOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
}
