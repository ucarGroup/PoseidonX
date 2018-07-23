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

package com.huawei.streaming.cql.executor.windowcreater;

import java.util.List;
import java.util.Map;

import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.operatorviewscreater.GroupByViewCreator;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.window.IWindow;
import com.huawei.streaming.window.sort.TimeSortWindow;

/**
 * 时间排序窗实例创建
 * 
 * 时间窗按照时间的先后顺序就进行排列，所以只能按照一个字段排序
 * 并且排列顺序一定是固定的。
 * 
 */
public class TimeSortWindowCreator implements WindowCreator
{
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IWindow createInstance(Window window, List<Schema> schemas, Map<String, String> systemConfig)
        throws ExecutorException
    {
        IExpression[] orderbyExpressions =
            new GroupByViewCreator().create(schemas, window.getOrderbyExpression(), systemConfig);
        
        return new TimeSortWindow(window.getLength(), orderbyExpressions[0]);
    }
}
