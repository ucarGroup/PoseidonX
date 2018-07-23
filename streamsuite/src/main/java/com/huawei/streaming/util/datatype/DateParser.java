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

import com.google.common.base.Strings;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * 日期类型解析
 * Created by h00183771 on 2015/11/18.
 */
public class DateParser implements DataTypeParser
{
    private static final long serialVersionUID = 6369934838377130426L;

    private static final Logger LOG = LoggerFactory.getLogger(DateParser.class);

    private final SimpleDateFormat dateFormatter;

    private final TimeZone timeZone;
    /**
     * <默认构造函数>
     */
    public DateParser(StreamingConfig config)
        throws StreamingException
    {
        Calendar cal = Calendar.getInstance();
        timeZone = cal.getTimeZone();
        dateFormatter = new SimpleDateFormat(TimeConstants.DATE_FORMAT);
        dateFormatter.setLenient(false);
        dateFormatter.setTimeZone(timeZone);
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

        if(value.length() >TimeConstants.DATE_FORMAT.length())
        {
            LOG.warn("Failed to create {} instance.", Date.class.getName());
            throw new StreamingException("Failed to create " + Date.class.getName() + " instance.");
        }

        try
        {
            java.util.Date uDate = dateFormatter.parse(value);
            return new Date(uDate.getTime());
        }
        catch (Exception e)
        {
            LOG.warn("Failed to create {} instance.", Date.class.getName());
            throw new StreamingException("Failed to create " + Date.class.getName() + " instance.");
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

        return dateFormatter.format(value);
    }

    /**
     * 获取时区
     */
    public TimeZone getTimeZone()
    {
        return timeZone;
    }
}
