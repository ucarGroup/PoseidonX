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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * 数据类型转换函数
 *
 */
@UDFAnnotation("todate")
public class ToDate extends UDF
{
    private static final long serialVersionUID = -4516472038115224500L;

    private static final Logger LOG = LoggerFactory.getLogger(ToDate.class);

    private final DateParser dateParser;

    private SimpleDateFormat dateFormat = null;

    /**
     * <默认构造函数>
     */
    public ToDate(Map<String, String> config)
        throws StreamingException
    {
        super(config);
        StreamingConfig conf = new StreamingConfig();
        for (Map.Entry<String, String> et : config.entrySet())
        {
            conf.put(et.getKey(), et.getValue());
        }
        dateParser = new DateParser(conf);
    }

    /**
     * 类型转换实现
     */
    public Date evaluate(String s)
    {
        if (Strings.isNullOrEmpty(s))
        {
            return null;
        }

        //默认支持yyyy-[m]m-[d]d格式
        try
        {
            return (Date)dateParser.createValue(s);
        }
        catch (StreamingException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }

    /**
     * 类型转换实现
     */
    public Date evaluate(String s, String format)
    {
        if (Strings.isNullOrEmpty(s) || Strings.isNullOrEmpty(format))
        {
            return null;
        }

        return eval(s, format);
    }

    private Date eval(String s, String format)
    {
        if (null == dateFormat)
        {
            dateFormat = new SimpleDateFormat(format);
            /*
             * 设置时间严格匹配
             */
            dateFormat.setLenient(false);
            dateFormat.setTimeZone(dateParser.getTimeZone());
        }

        java.util.Date date = null;
        try
        {
            date = dateFormat.parse(s);
        }
        catch (ParseException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }

        return new Date(date.getTime());
    }

}
