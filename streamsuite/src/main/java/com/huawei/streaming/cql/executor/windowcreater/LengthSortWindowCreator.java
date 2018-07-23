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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.operatorinfocreater.AggregaterInfoCreator;
import com.huawei.streaming.cql.executor.operatorviewscreater.GroupByViewCreator;
import com.huawei.streaming.cql.executor.operatorviewscreater.OrderByViewCreator;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.process.sort.SortCondition;
import com.huawei.streaming.process.sort.SortEnum;
import com.huawei.streaming.window.IWindow;
import com.huawei.streaming.window.sort.LengthSortWindow;

/**
 * 创建长度排序窗口实例
 *
 */
public class LengthSortWindowCreator implements WindowCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(LengthSortWindowCreator.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public IWindow createInstance(Window window, List< Schema > schemas, Map< String, String > systemConfig)
     throws ExecutorException
    {
        /**
         * 长度排序窗口默认降序，只有这样才可以保证旧数据先出窗口
         */
        List< SortCondition > condition = new OrderByViewCreator().create(schemas, window.getOrderbyExpression());
        if (condition == null || condition.size() == 0 || condition.get(0) == null)
        {
            ExecutorException exception = new ExecutorException(ErrorCode.WINDOW_UNSUPPORTED_PARAMETERS);
            LOG.error("Unsupported parameters when create window ", exception);
            throw exception;
        }

        condition.get(0).setSortEnum(SortEnum.DESC);

        return new LengthSortWindow(window.getLength().intValue(), condition);

    }
}
