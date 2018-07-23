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

package com.huawei.streaming.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.datatype.BooleanParser;
import com.huawei.streaming.util.datatype.DataTypeParser;
import com.huawei.streaming.util.datatype.DateParser;
import com.huawei.streaming.util.datatype.DecimalParser;
import com.huawei.streaming.util.datatype.DoubleParser;
import com.huawei.streaming.util.datatype.FloatParser;
import com.huawei.streaming.util.datatype.IntParser;
import com.huawei.streaming.util.datatype.LongParser;
import com.huawei.streaming.util.datatype.StringParser;
import com.huawei.streaming.util.datatype.TimeParser;
import com.huawei.streaming.util.datatype.TimestampParser;

/**
 * Streaming数据类型
 *
 */
public enum StreamingDataType
{

    /**
     * 字符串数据类型
     */
    STRING("STRING", String.class, null)
     {
         /**
          * {@inheritDoc}
          */
         @Override
         public DataTypeParser createParser(StreamingConfig conf) throws StreamingException
         {
             return new StringParser();
         }
     },

    /**
     * Integer数据类型
     */
    INT("INT", Integer.class, int.class)
     {
         /**
          * {@inheritDoc}
          */
         @Override
         public DataTypeParser createParser(StreamingConfig conf) throws StreamingException
         {
             return new IntParser();
         }
     },

    /**
     * long数据类型
     */
    LONG("LONG", Long.class, long.class)
     {
         /**
          * {@inheritDoc}
          */
         @Override
         public DataTypeParser createParser(StreamingConfig conf) throws StreamingException
         {
             return new LongParser();
         }
     },

    /**
     * float数据类型
     */
    FLOAT("FLOAT", Float.class, float.class)
     {
         /**
          * {@inheritDoc}
          */
         @Override
         public DataTypeParser createParser(StreamingConfig conf) throws StreamingException
         {
             return new FloatParser();
         }
     },

    /**
     * double数据类型
     */
    DOUBLE("DOUBLE", Double.class, double.class)
     {
         /**
          * {@inheritDoc}
          */
         @Override
         public DataTypeParser createParser(StreamingConfig conf) throws StreamingException
         {
             return new DoubleParser();
         }
     },

    /**
     * boolean数据类型
     */
    BOOLEAN("BOOLEAN", Boolean.class, boolean.class)
     {
         /**
          * {@inheritDoc}
          */
         @Override
         public DataTypeParser createParser(StreamingConfig conf) throws StreamingException
         {
             return new BooleanParser();
         }
     },

    /**
     * 时间戳类型
     */
    TIMESTAMP("TIMESTAMP", Timestamp.class, null)
     {
         /**
          * {@inheritDoc}
          */
         @Override
         public DataTypeParser createParser(StreamingConfig conf) throws StreamingException
         {
             return new TimestampParser(conf);
         }
     },

    /**
     * 日期类型
     */
    DATE("DATE", Date.class, null)
     {
         /**
          * {@inheritDoc}
          */
         @Override
         public DataTypeParser createParser(StreamingConfig conf) throws StreamingException
         {
             return new DateParser(conf);
         }
     },

    /**
     * 时间类型
     */
    TIME("TIME", Time.class, null)
     {
         /**
          * {@inheritDoc}
          */
         @Override
         public DataTypeParser createParser(StreamingConfig conf) throws StreamingException
         {
             return new TimeParser(conf);
         }
     },

    /**
     * decimal类型
     */
    DECIMAL("DECIMAL", BigDecimal.class, null)
     {
         /**
          * {@inheritDoc}
          */
         @Override
         public DataTypeParser createParser(StreamingConfig conf)
         {
             return new DecimalParser();
         }
     };

    private static final Logger LOG = LoggerFactory.getLogger(StreamingDataType.class);

    /**
     * 数据类型在CQL中的描述
     * 所以该描述内容不允许随意修改
     */
    private String desc;

    /**
     * 包装类型
     * 包装类型不能为空
     */
    private Class< ? > wrapperClass;

    /**
     * 原始类型
     */
    private Class< ? > innerClass;

    StreamingDataType(String t, Class< ? > wrapperClazz, Class< ? > innerClazz)
    {
        this.desc = t;
        this.wrapperClass = wrapperClazz;
        this.innerClass = innerClazz;
    }

    public String getDesc()
    {
        return desc;
    }

    public Class< ? > getWrapperClass()
    {
        return wrapperClass;
    }

    public Class< ? > getInnerClass()
    {
        return innerClass;
    }

    /**
     * 是否和当前枚举类型书类型相符
     *
     */
    private boolean isEqualDataType(Class< ? > clazz)
    {
        return clazz.equals(wrapperClass) || clazz.equals(innerClass);
    }



    /**
     * 通过Class获取数据类型
     */
    public static DataTypeParser getDataTypeParser(Class< ? > clazz, StreamingConfig conf)
     throws StreamingException
    {
        for (StreamingDataType dataType : StreamingDataType.values())
        {
            if (dataType.isEqualDataType(clazz))
            {
                return dataType.createParser(conf);
            }
        }

        StreamingException exception =
         new StreamingException(ErrorCode.SEMANTICANALYZE_UNSUPPORTED_DATATYPE, clazz.getName());
        LOG.error("UnSupport data type.", exception);
        throw exception;
    }

    /**
     * 通过Class获取数据类型
     */
    public static StreamingDataType getStreamingDataType(Class< ? > clazz)
     throws StreamingException
    {
        for (StreamingDataType dataType : StreamingDataType.values())
        {
            if (dataType.isEqualDataType(clazz))
            {
                return dataType;
            }
        }

        StreamingException exception =
         new StreamingException(ErrorCode.SEMANTICANALYZE_UNSUPPORTED_DATATYPE, clazz.getName());
        LOG.error("UnSupport data type.", exception);
        throw exception;
    }


    /**
     * 创建数据类型解析实例
     */
    public abstract DataTypeParser createParser(StreamingConfig conf) throws StreamingException;
}
