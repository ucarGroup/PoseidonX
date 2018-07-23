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

/**
 * 数据类型转换函数
 * 
 */
@UDFAnnotation("toboolean")
public class ToBoolean extends UDF
{
    private static final long serialVersionUID = -4516472038115224500L;
    
    
    /**
     * <默认构造函数>
     */
    public ToBoolean(Map<String, String> config)
    {
        super(config);
    }
    
    /**
     * 类型转换实现
     */
    public Boolean evaluate(Boolean i)
    {
        return i;
    }
    
    
    /**
     * 类型转换实现
     */
    public Boolean evaluate(Integer i)
    {
        if (i == null)
        {
            return null;
        }
        else
        {
            return i != 0;
        }
    }
    
    /**
     * 类型转换实现
     */
    public Boolean evaluate(Long i)
    {
        if (i == null)
        {
            return null;
        }
        else
        {
            return i.longValue() != 0;
        }
    }
    
    /**
     * 类型转换实现
     */
    public Boolean evaluate(Float i)
    {
        if (i == null)
        {
            return null;
        }
        else
        {
            return i.longValue() != 0;
        }
    }
    
    /**
     * 类型转换实现
     */
    public Boolean evaluate(Double i)
    {
        if (i == null)
        {
            return null;
        }
        else
        {
            return i.longValue() != 0;
        }
    }
    
    /**
     * 类型转换实现
     */
    public Boolean evaluate(BigDecimal i)
    {
        if (i == null)
        {
            return null;
        }
        else
        {
            return i.longValue() != 0;
        }
    }
    
    /**
     * 类型转换实现
     */
    public Boolean evaluate(String i)
    {
        if (i == null)
        {
            return null;
        }
        else
        {
            return i.length() != 0;
        }
    }
    
    /**
     * 类型转换实现
     */
    public Boolean evaluate(Time i)
    {
        if (i == null)
        {
            return null;
        }
        else
        {
            return i.getTime() != 0;
        }
    }
    
    /**
     * 类型转换实现
     */
    public Boolean evaluate(Date i)
    {
        if (i == null)
        {
            return null;
        }
        else
        {
            return i.getTime() != 0;
        }
    }
    
    /**
     * 类型转换实现
     */
    public Boolean evaluate(Timestamp i)
    {
        if (i == null)
        {
            return null;
        }
        else
        {
            return i.getTime() != 0;
        }
    }
    
}
