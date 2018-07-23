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
package com.huawei.streaming.util.datatype;

/**
 * UDF函数中使用到的常量
 *
 */
public class TimeConstants
{
    /**
     * date类型的字符串格式
     */
    public static final String DATE_FORMAT = "yyyy-MM-dd";

    /**
     * Time类型字符串格式
     */
    public static final String TIME_FORMAT = "HH:mm:ss";

    /**
     * timestamp字符串格式
     */
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 带时区的timestamp格式
     */
    public static final String TIMESTAMP_TIMEZONE_FORMAT = "yyyy-MM-dd HH:mm:ss Z";


    /**
     * 带纳秒的时间戳字段
     * 一般用于格式化输出
     * 只能精确到毫秒
     */
    public static final String TIMESTAMP_MSTIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    /**
     * 带纳秒的时间戳字段
     * 一般用于格式化输出
     * 只能精确到毫秒
     * 带时区
     */
    public static final String TIMESTAMP_MSTIME_TIMEZONE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS Z";


    public static final String ZERO_TIMEZONE = "GMT+00:00";
}
