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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamingException;

/**
 * timestamp类型解析
 * Created by h00183771 on 2015/11/18.
 */
public class TimestampParser implements DataTypeParser
{
    private static final long serialVersionUID = 9221885152016950640L;

    private static final Logger LOG = LoggerFactory.getLogger(TimestampParser.class);

    /**
     * 默认时区
     * 该参数一般由客户端在提交任务之前设置一次
     */

    private SimpleDateFormat outputFormatter;

    /*
     * timestamp类型允许输入带毫秒，纳秒的数字
     * 所以格式精确到秒级别。
     */
    private SimpleDateFormat inputFormatter;
    
    /*
     * 带timezone的输入格式。
     */
    private SimpleDateFormat inputFormatterTZ;

    /*
     * 带ms的输入格式。
     */
    private SimpleDateFormat inputFormatterMS;

    /*
     * 带timezone和ms的输入格式。
     */
    private SimpleDateFormat inputFormatterMSTZ;

    private final TimeZone timeZone;

    /**
     * <默认构造函数>
     */
    public TimestampParser(StreamingConfig config) throws StreamingException
    {
        timeZone = TimeZoneUtils.getTimeZone(config);
        initDefaultInputFormat(timeZone);
        initOutputFormat(timeZone);
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

        try
        {
            Date date = parseDateFormatFromInput(value).parse(value);
            return new Timestamp(date.getTime());
        }
        catch (Exception e)
        {
            LOG.warn("Failed to create {} instance.", Timestamp.class.getName());
            throw new StreamingException("Failed to create " + Timestamp.class.getName() + " instance.");
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

        return outputFormatter.format(value);
    }

    /**
     * 获取时区
     */
    public TimeZone getTimeZone()
    {
        return timeZone;
    }

    private void initDefaultInputFormat(TimeZone timeZone)
    {
        inputFormatter = new SimpleDateFormat(TimeConstants.TIMESTAMP_FORMAT);
        inputFormatter.setLenient(false);
        inputFormatter.setTimeZone(timeZone);

        inputFormatterTZ = new SimpleDateFormat(TimeConstants.TIMESTAMP_TIMEZONE_FORMAT);
        inputFormatterTZ.setLenient(false);
        inputFormatterTZ.setTimeZone(timeZone);

        inputFormatterMS = new SimpleDateFormat(TimeConstants.TIMESTAMP_MSTIME_FORMAT);
        inputFormatterMS.setLenient(false);
        inputFormatterMS.setTimeZone(timeZone);

        inputFormatterMSTZ = new SimpleDateFormat(TimeConstants.TIMESTAMP_MSTIME_TIMEZONE_FORMAT);
        inputFormatterMSTZ.setLenient(false);
        inputFormatterMSTZ.setTimeZone(timeZone);
    }

    private void initOutputFormat(TimeZone timeZone) throws StreamingException
    {
        outputFormatter = new SimpleDateFormat(TimeConstants.TIMESTAMP_MSTIME_TIMEZONE_FORMAT);
        outputFormatter.setLenient(false);
        outputFormatter.setTimeZone(timeZone);
    }


    private SimpleDateFormat parseDateFormatFromInput(String input)
    {
        //点号出现的索引，如果没有出现点号，则说明不包含毫秒
        int pointIndex = -1;
        //空格出现的次数，如果有两个空格，则说明带时区
        int blankCount = 0;

        char[] inputChars = input.toCharArray();
        for (int i = 0; i < inputChars.length; i++)
        {
            switch (inputChars[i])
            {
                case '.':
                    pointIndex = i;
                    break;
                case ' ':
                    blankCount++;
                    break;
                default:
                    break;
            }
        }

        //带毫秒
        if (pointIndex != -1)
        {
            //带时区
            if (blankCount == 2)
            {
                return inputFormatterMSTZ;
            }
            else
            {
                return inputFormatterMS;
            }
        }
        else
        {
            //带时区
            if (blankCount == 2)
            {
                return inputFormatterTZ;
            }
            else
            {
                return inputFormatter;
            }
        }
    }
}
