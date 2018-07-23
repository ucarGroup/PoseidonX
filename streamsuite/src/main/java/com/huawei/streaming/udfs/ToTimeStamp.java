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

import java.sql.Timestamp;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.datatype.TimestampParser;

/**
 * 数据类型转换函数
 * 
 */
@UDFAnnotation("totimestamp")
public class ToTimeStamp extends UDF
{
    private static final long serialVersionUID = -4516472038115224500L;
    
    private static final Logger LOG = LoggerFactory.getLogger(ToTimeStamp.class);
    private TimestampParser timestampParser = null;

    /**
     * <默认构造函数>
     */
    public ToTimeStamp(Map<String, String> config) throws StreamingException
    {
        super(config);
        StreamingConfig conf = new StreamingConfig();
        for(Map.Entry<String, String> et : config.entrySet())
        {
            conf.put(et.getKey(), et.getValue());
        }
        timestampParser = new TimestampParser(conf);
    }
    
    /**
     * 类型转换实现
     */
    public Timestamp evaluate(String s)
    {
        if (Strings.isNullOrEmpty(s))
        {
            return null;
        }
        
        try
        {
            /**
             *  更改该函数默认行为和输入一致
             */
            return (Timestamp)timestampParser.createValue(s);
        }
        catch (StreamingException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }
}
