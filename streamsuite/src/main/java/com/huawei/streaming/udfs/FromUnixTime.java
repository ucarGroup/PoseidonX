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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.datatype.TimeConstants;
import com.huawei.streaming.util.datatype.TimeZoneUtils;

/**
 * 
 * 对long类型的数据进行格式化，转化为对应的字符串形式的时间
 * SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss') FROM src LIMIT 1;'1970-01-01 00:00:00'")
 */
@UDFAnnotation("from_unixtime")
public class FromUnixTime extends UDF
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -9008532111693346031L;

    private static final Logger LOG = LoggerFactory.getLogger(FromUnixTime.class);

    private static final long ONE_SECOND = 1000L;
    
    private static final String DEFAULT_FORMAT = TimeConstants.TIMESTAMP_FORMAT;
    
    private SimpleDateFormat formatter;
    
    private String lastFormat = null;

    private StreamingConfig conf;

    /**
     * <默认构造函数>
     */
    public FromUnixTime(Map<String, String> config)
    {
        super(config);
        conf = new StreamingConfig();
        for(Map.Entry<String, String> et : config.entrySet())
        {
            conf.put(et.getKey(), et.getValue());
        }

    }
    
    /**
     * 使用默认的格式对long类型的时间戳进行格式化
     */
    public String evaluate(Long unixtime)
    {
        if (unixtime == null)
        {
            return null;
        }
        try
        {
            return eval(unixtime, DEFAULT_FORMAT);
        }
        catch (StreamingException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }
    
    /**
     * 使用指定的格式对long类型的时间进行格式化
     */
    public String evaluate(Long unixtime, String format)
    {
        if (unixtime == null || format == null)
        {
            return null;
        }
        
        try
        {
            return eval(unixtime, format);
        }
        catch (StreamingException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }
    
    /**
     * 使用指定的格式对int类型的时间进行格式化
     */
    public String evaluate(Integer unixtime, String format)
    {
        if (unixtime == null || format == null)
        {
            return null;
        }
        
        try
        {
            return eval(Long.valueOf(unixtime), format);
        }
        catch (StreamingException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }
    
    private String eval(long unixtime, String format) throws StreamingException
    {
        if (!format.equals(lastFormat))
        {
            formatter = new SimpleDateFormat(format);
            /*
             * 设置时间严格匹配
             */
            formatter.setLenient(false);
            formatter.setTimeZone(TimeZoneUtils.getTimeZone(getConfig()));
            lastFormat = format;
        }
        
        // convert seconds to milliseconds
        Date date = new Date(unixtime);
        return formatter.format(date);
    }
}
