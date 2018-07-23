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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据类型转换函数
 * 
 */
@UDFAnnotation("todecimal")
public class ToDecimal extends UDF
{
    private static final long serialVersionUID = -4516472038115224500L;

    private static final Logger LOG = LoggerFactory.getLogger(ToDecimal.class);

    /**
     * <默认构造函数>
     */
    public ToDecimal(Map< String, String > config)
    {
        super(config);
    }

    /**
     * 类型转换实现
     */
    public BigDecimal evaluate(String s)
    {
        try
        {
            return BigDecimal.valueOf(Double.valueOf(s));
        }
        catch (NumberFormatException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }

    /**
     * 类型转换实现
     */
    public BigDecimal evaluate(int s)
    {
        return BigDecimal.valueOf(s);
    }
    
    /**
     * 类型转换实现
     */
    public BigDecimal evaluate(long s)
    {
        try
        {
            return BigDecimal.valueOf(s);
        }
        catch (NumberFormatException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }
    
    /**
     * 类型转换实现
     */
    public BigDecimal evaluate(long s, int scale)
    {
        try
        {
            return BigDecimal.valueOf(s, scale);
        }
        catch (NumberFormatException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }
    
    /**
     * 类型转换实现
     */
    public BigDecimal evaluate(Double s)
    {
        try
        {
            return BigDecimal.valueOf(s);
        }
        catch (NumberFormatException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }
    
    /**
     * 类型转换实现
     */
    public BigDecimal evaluate(Float s)
    {
        try
        {
            return BigDecimal.valueOf(s);
        }
        catch (NumberFormatException e)
        {
            LOG.warn(EVALUATE_IGNORE_MESSAGE);
            return null;
        }
    }
}
