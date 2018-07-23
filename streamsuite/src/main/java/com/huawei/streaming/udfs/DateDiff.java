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
import java.util.Date;
import java.util.Map;

/**
 * 比较两个时间相差的天数
 *
 */
@UDFAnnotation("datediff")
public class DateDiff extends UDF
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -5597820284474479464L;

    private static final Logger LOG = LoggerFactory.getLogger(DateDiff.class);

    private static final int ONE_SECOND = 1000;

    private static final int ONE_DAY = 24 * 60 * 60 * ONE_SECOND;

    private DateParser dateParser;

    private TimestampParser timestampParser;

    /**
     * <默认构造函数>
     *
     */
    public DateDiff(Map< String, String > config)
        throws StreamingException
    {
        super(config);
        StreamingConfig conf = new StreamingConfig();
        for(Map.Entry<String, String> et : config.entrySet())
        {
            conf.put(et.getKey(), et.getValue());
        }
        dateParser = new DateParser(conf);
        timestampParser = new TimestampParser(conf);
    }

    /**
     * Calculate the difference in the number of days. The time part of the string
     * will be ignored. If dateString1 is earlier than dateString2, then the
     * result can be negative.
     *
     * "yyyy-MM-dd".
     * "yyyy-MM-dd".
     */
    public Integer evaluate(String str1, String str2)
    {
        if (Strings.isNullOrEmpty(str1) || Strings.isNullOrEmpty(str2))
        {
            return null;
        }

        try
        {
            long date1 = toDate(str1);
            long date2 = toDate(str2);
            return Long.valueOf((date1-date2) / ONE_DAY).intValue();
        }
        catch (StreamingException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }

    }

    private Long toDate(String dateString)
        throws StreamingException
    {
        if (dateString == null)
        {
            return null;
        }
        return format(dateString);
    }

    private Long format(String dateString)
        throws StreamingException
    {
        if (dateString.length() > TimeConstants.DATE_FORMAT.length())
        {
            /*
             * timestamp时间日期计算方式
             * 1、将时间转为timestamp类型，避免时区问题
             * 2、将timestamp类型时间转为字符串类型
             * 3、从字符串类型格式中截取日期。
             * 4、创建date类型，这个时候的日期就变成了0时区
             * JDK和其它框架中只能比较两个日期之间相差的毫秒数，无法比较相差天数。
             */
            String newDateString = timestampParser.toStringValue(timestampParser.createValue(dateString));
            int strDateIndex = newDateString.indexOf(' ');
            Date dt = (Date)dateParser.createValue(newDateString.substring(0, strDateIndex));
            return dt.getTime();
        }
        else
        {
            Date dt = (Date)dateParser.createValue(dateString);
            return dt.getTime();
        }
    }
}
