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

package com.huawei.streaming.serde;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * 基于tcp的传输处理
 * 完成Strom中的spout功能
 */
public class CSVLineParser
{
    private static CSVParser parser = new CSVParser();
    
    /**
     * <解析CVS一行数据>
     * <功能详细描述>
     */
    public static List<Object[]> parseCsv(String obj)
        throws IOException
    {
        List<Object[]> arrs = Lists.newArrayList();
        String[] result = null;
        if (obj != null)
        {
            Object[] r = parser.parseLineMulti(obj);
            if (r.length > 0)
            {
                arrs.add(r);
            }
        }
        return arrs;
    }
}
