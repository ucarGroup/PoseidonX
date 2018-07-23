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

package com.huawei.streaming.process.agg.aggregator;

import com.huawei.streaming.process.agg.aggregator.sum.AggregateSum;
import com.huawei.streaming.view.IView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * <聚合操作抽象类>
 * <提供抽象类，方便解析层采用相同的方式进行构造聚合操作，包括内部定义和用户自定义>
 * 
 */
public abstract class AbstractAggregate implements IAggregate, IAggregateClone
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 1994679423096368200L;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractAggregate.class);

    /**
     * <默认构造函数>
     *@param type 聚合操作返回值类型
     */
    public AbstractAggregate(Class< ? > type)
    {
    }
}
