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

import java.util.Set;

import com.google.common.collect.Sets;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.opereators.WindowCommons;
import com.huawei.streaming.output.OutputType;

/**
 * 输出类型分析
 * 
 */
public class OutputTypeAnalyzer
{
    /**
     * 输出为R流的窗口
     */
    private static final Set<String> RSTREAM_WINDOW = Sets.newHashSet(WindowCommons.LENGTH_SORT_WINDOW,
        WindowCommons.TIME_SORT_WINDOW);
    
    /**
     * 判断输出类型
     * 带排序窗口的都是R流输出
     */
    protected static OutputType createOutputType(Window window)
    {
        return isRStreamWindow(window) ? OutputType.R : OutputType.I;
    }
    
    /**
     * 判断是否是R流输出的窗口
     */
    protected static boolean isRStreamWindow(Window window)
    {
        if(window == null)
        {
            return false;
        }
        return RSTREAM_WINDOW.contains(window.getName());
    }
}
