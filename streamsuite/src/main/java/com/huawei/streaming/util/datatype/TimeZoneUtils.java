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

import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.ReflectUtil;

/**
 * 时区格式化类
 * TimeZone中的格式化方法，会把错误的时区也识别成0时区，所以没办法判断输入参数是否正常
 * Created by h00183771 on 2015/10/20.
 */
public class TimeZoneUtils
{
    private static final Logger LOG = LoggerFactory.getLogger(TimeZoneUtils.class);

    /**
     * 解析时区字符串，如果错误则返回空
     * 支持GMT+08:00格式和Asia/Shanghai这样的时区格式
     * 如果格式错误，统一返回null
     * 1、不能直接使用ZoneInfo类，这个是java内部类，编译的时候会提示，可能会过期
     * 2、TimeZone自身公开静态类，会在传入的字符串错误的情况下返回0时区，这样就无法判断参数错误
     * 3、该方法使用反射，不建议频繁一调用
     */
    public static TimeZone parseTimeZone(String zone) throws StreamingException
    {
        try
        {
            return ReflectUtil.on(TimeZone.class).invoke("getTimeZone", zone, false).get();
        }
        catch (ReflectiveOperationException e)
        {
            LOG.error("Failed to parse timezone {}.", zone);
            throw new StreamingException("Failed to parse timezone " + zone);
        }
    }

    /**
     * 获取时区
     */
    public  static TimeZone getTimeZone(StreamingConfig config) throws StreamingException
    {
        String strZone = config.getStringValue(StreamingConfig.STREAMING_OPERATOR_TIMEZONE);
        TimeZone timeZone = TimeZoneUtils.parseTimeZone(strZone);
        if (timeZone == null)
        {
            LOG.error(ErrorCode.TOP_ERROR_TIME_ZONE.getFullMessage(strZone));
            throw new StreamingException(ErrorCode.TOP_ERROR_TIME_ZONE, strZone);
        }
        return timeZone;
    }

    /**
     * 获取时区
     */
    public  static TimeZone getTimeZone(Map<String, String> config) throws StreamingException
    {
        String strZone = config.get(StreamingConfig.STREAMING_OPERATOR_TIMEZONE);
        TimeZone timeZone = TimeZoneUtils.parseTimeZone(strZone);
        if (timeZone == null)
        {
            LOG.error(ErrorCode.TOP_ERROR_TIME_ZONE.getFullMessage(strZone));
            throw new StreamingException(ErrorCode.TOP_ERROR_TIME_ZONE, strZone);
        }
        return timeZone;
    }
}
