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

import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamingException;

/**
 * 数据类型解析
 * Created by h00183771 on 2015/11/18.
 */
public class TimeParser implements DataTypeParser
{
    private static final long serialVersionUID = -3848004186444584927L;

    private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);

    private final SimpleDateFormat timeFormatter;

    private final TimeZone timeZone;
    /**
     * <默认构造函数>
     */
    public TimeParser(StreamingConfig config) throws StreamingException
    {
        timeZone = TimeZoneUtils.parseTimeZone(TimeConstants.ZERO_TIMEZONE);
        timeFormatter = new SimpleDateFormat(TimeConstants.TIME_FORMAT);
        timeFormatter.setTimeZone(timeZone);
        timeFormatter.setLenient(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object createValue(String value)
     throws StreamingException
    {
        if (Strings.isNullOrEmpty(value))
        {
            return null;
        }

        if(value.length() >TimeConstants.TIME_FORMAT.length())
        {
            LOG.warn("Failed to create {} instance.", Date.class.getName());
            throw new StreamingException("Failed to create " + Date.class.getName() + " instance.");
        }

        try
        {
             java.util.Date uDate = timeFormatter.parse(value);
             return new Time(uDate.getTime());
        }
        catch (Exception e)
        {
            //序列化反序列化异常使用warn级别，因为仅仅会导致当前数据丢掉，不会导致进程退出
            LOG.warn("Failed to create {} instance.", Time.class.getName());
            throw new StreamingException("Failed to create " + Time.class.getName() + " instance.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toStringValue(Object value)
     throws StreamingException
    {
        if (null == value)
        {
            return null;
        }

        return timeFormatter.format(new Date(((Time) value).getTime()));
    }

    /**
     * 获取时区
     */
    public TimeZone getTimeZone()
    {
        return timeZone;
    }
}
