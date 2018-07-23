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

package com.huawei.streaming.cql.semanticanalyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.StreamingDataType;

/**
 * 常量公共方法
 *
 */
public class ConstUtils
{
    private static final Logger LOG = LoggerFactory.getLogger(ConstUtils.class);
    
    /**
     * int格式化
     *
     */
    public static Integer formatInt(String number)
        throws SemanticAnalyzerException
    {
        try
        {
            return Integer.valueOf(number);
        }
        catch (NumberFormatException e)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_CONSTANT_FORMAT, number, "INT");
            LOG.error("Faild to format to Int.", exception);
            
            throw exception;
        }
    }
    
    /**
     * 获取字符串类型的数据类型
     *
     */
    public static String getDataType(Class< ? > type)
    {
        try
        {
            StreamingDataType dataType =  StreamingDataType.getStreamingDataType(type);
            return dataType.getDesc();
        }
        catch (StreamingException e)
        {
            LOG.error("Can't get streaming data type.");
        }

        return null;
    }
}
