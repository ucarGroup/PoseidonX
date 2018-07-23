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
package com.huawei.streaming.cql.executor;

import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

/**
 * 将RDB数据源中的敏感信息在序列化的时候隐藏起来
 *
 */
public class RDBSecurityConverter implements Converter
{

    /**
     * 被替换的字符串
     */
    private static final String REPLACED_STRING = "******";

    /**
     * {@inheritDoc}
     */
    @Override
    public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context)
    {
        writer.setValue(REPLACED_STRING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context)
    {
        return reader.getValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canConvert(Class type)
    {
        return true;
    }
}
