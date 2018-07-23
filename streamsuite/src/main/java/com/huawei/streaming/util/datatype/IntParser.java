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

import com.google.common.base.Strings;
import com.huawei.streaming.exception.StreamingException;

/**
 * 数据类型解析
 * Created by h00183771 on 2015/11/18.
 */
public class IntParser implements DataTypeParser
{
    private static final long serialVersionUID = -1586409475994912510L;

    private static final Logger LOG = LoggerFactory.getLogger(IntParser.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public Object createValue(String value)
     throws StreamingException
    {
        if (Strings.isNullOrEmpty(value))
        {
            return null;
        }

        try
        {
            return Integer.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            LOG.warn("Failed to create {} instance.", Integer.class.getName());
            throw new StreamingException("Failed to create " + Integer.class.getName() + " instance.");
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
