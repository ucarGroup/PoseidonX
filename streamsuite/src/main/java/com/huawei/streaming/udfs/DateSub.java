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
package com.huawei.streaming.udfs;

import com.google.common.base.Strings;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.datatype.DateParser;
import com.huawei.streaming.util.datatype.TimeConstants;
import com.huawei.streaming.util.datatype.TimestampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * 时间相减
 *
 */
@UDFAnnotation("datesub") public class DateSub extends UDF
{

    private static final long serialVersionUID = -8498030708655301150L;

    private static final Logger LOG = LoggerFactory.getLogger(DateSub.class);

    private DateParser dateParser;

    private TimestampParser timestampParser;

    private Calendar calendar = null;

    private Boolean isTimestampType = null;

    /**
     * <默认构造函数>
     *
     */
    public DateSub(Map<String, String> config)
        throws StreamingException
    {
        super(config);
        StreamingConfig conf = new StreamingConfig();
        for (Map.Entry<String, String> et : config.entrySet())
        {
            conf.put(et.getKey(), et.getValue());
        }
        dateParser = new DateParser(conf);
        timestampParser = new TimestampParser(conf);
    }

    /**
     * 计算函数
     *
     */
    public String evaluate(String dateString, Integer days)
    {
        if (Strings.isNullOrEmpty(dateString) || days == null)
        {
            return null;
        }

        if (isTimestampType == null)
        {
            initDataType(dateString);
            calendar = Calendar.getInstance();
            if (isTimestampType)
            {
                calendar.setTimeZone(timestampParser.getTimeZone());
            }
            else
            {
                calendar.setTimeZone(dateParser.getTimeZone());
            }
        }

        try
        {
            calendar.setTimeInMillis(getTime(dateString));
            calendar.add(Calendar.DAY_OF_MONTH, 0 - days);
            if (isTimestampType)
            {
                return timestampParser.toStringValue(new Timestamp(calendar.getTimeInMillis()));
            }
            else
            {
                return dateParser.toStringValue(new java.sql.Date(calendar.getTimeInMillis()));
            }
        }
        catch (StreamingException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }

    /**
     * 计算函数
     *
     */
    public String evaluate(Timestamp ts, Integer days)
    {
        if (ts == null || days == null)
        {
            return null;
        }

        if (isTimestampType == null)
        {
            initDataType(null);
            calendar = Calendar.getInstance();
            isTimestampType = true;
            calendar.setTimeZone(timestampParser.getTimeZone());
        }

        try
        {
            calendar.setTime(ts);
            calendar.add(Calendar.DAY_OF_MONTH, 0 - days);
            return timestampParser.toStringValue(new Timestamp(calendar.getTimeInMillis()));
        }
        catch (StreamingException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }

    private long getTime(String dateString)
        throws StreamingException
    {
        if (isTimestampType)
        {
            Timestamp ts = (Timestamp)timestampParser.createValue(dateString);
            return ts.getTime();
        }
        else
        {
            Date dt = (Date)dateParser.createValue(dateString);
            return dt.getTime();
        }
    }

    private void initDataType(String dateString)
    {
        if (dateString == null)
        {
            return;
        }

        if (dateString.length() > TimeConstants.DATE_FORMAT.length())
        {
            isTimestampType = true;
        }
        else
        {
            isTimestampType = false;
        }
    }

}
