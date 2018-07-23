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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据类型转换函数
 * 
 */
@UDFAnnotation("toint")
public class ToInt extends UDF
{
    private static final long serialVersionUID = -4516472038115224500L;

    private static final Logger LOG = LoggerFactory.getLogger(ToInt.class);

    /**
     * <默认构造函数>
     */
    public ToInt(Map<String, String> config)
    {
        super(config);
    }

    /**
     * 类型转换实现
     *
     */
    public Integer evaluate(String s)
    {
        if (s == null)
        {
            return null;
        }

        try
        {
            return Integer.valueOf(s);
        }
        catch (NumberFormatException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }

    /**
     * 类型转换实现
     *
     */
    public Integer evaluate(Long s)
    {
        if (s == null)
        {
            return null;
        }

        return s.intValue();
    }

    /**
     * 类型转换实现
     *
     */
    public Integer evaluate(Integer s)
    {
        return s;
    }

    /**
     * 类型转换实现
     *
     */
    public Integer evaluate(Timestamp s)
    {
        if (s == null)
        {
            return null;
        }
        return evaluate(s.getTime());
    }

    /**
     * 类型转换实现
     *
     */
    public Integer evaluate(BigDecimal s)
    {
        if (s == null)
        {
            return null;
        }
        return s.intValue();
    }

    /**
     * 类型转换实现
     *
     */
    public Integer evaluate(Date s)
    {
        if (s == null)
        {
            return null;
        }

        return evaluate(s.getTime());
    }

    /**
     * 类型转换实现
     *
     */
    public Integer evaluate(Time s)
    {
        if (s == null)
        {
            return null;
        }
        return evaluate(s.getTime());
    }

    /**
     * 类型转换实现
     *
     */
    public Integer evaluate(Float s)
    {
        if (s == null)
        {
            return null;
        }
        return s.intValue();
    }

    /**
     * 类型转换实现
     *
     */
    public Integer evaluate(Double s)
    {
        if (s == null)
        {
            return null;
        }
        return s.intValue();
    }

}
