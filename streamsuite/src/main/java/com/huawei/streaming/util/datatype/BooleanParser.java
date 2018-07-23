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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.exception.StreamingException;

/**
 * Boolean数据类型解析
 * Created by h00183771 on 2015/11/18.
 */
public class BooleanParser implements DataTypeParser
{
    private static final long serialVersionUID = 5496611714066768093L;

    private static final Logger LOG = LoggerFactory.getLogger(BooleanParser.class);

    /**
     * 常量true
     */
    private static final String CONST_TRUE = "true";

    /**
     * 常量false
     */
    private static final String CONST_FALSE = "false";


    /**
     * {@inheritDoc}
     */
    @Override
    public Object createValue(String value)
     throws StreamingException
    {
            /*
             * 和Hive保持一致
             */
        if (value == null)
        {
            return null;
        }

        if (CONST_TRUE.equalsIgnoreCase(value))
        {
            return true;
        }
        else if (CONST_FALSE.equalsIgnoreCase(value))
        {
            return false;
        }
        else
        {
            LOG.debug("Data not in the boolean data type range so converted to null.");
            return null;
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
        return value.toString();
    }
}
