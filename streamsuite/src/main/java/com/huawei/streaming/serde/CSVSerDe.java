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

package com.huawei.streaming.serde;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;

/**
 * CSV格式的反序列化
 *
 */
public class CSVSerDe extends BaseSerDe
{
    private static final long serialVersionUID = 1094171706255989269L;

    private static final Logger LOG = LoggerFactory.getLogger(CSVSerDe.class);

    private List< Object[] > nullResults = Lists.newArrayList();

    public static final String LINE_SEPARATOR_UNIX = "\n";

    private CSVWriter csvCreator = new CSVWriter(this);

    private StringBuilder sb = new StringBuilder();

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException
    {
        super.setConfig(conf);
    }

    /**
     * 将原始数据按照设定格式分解
     *
     */
    @Override
    public List< Object[] > deSerialize(Object data)
        throws StreamSerDeException
    {

        if (data == null)
        {
            LOG.debug("Input raw data is null.");
            return nullResults;
        }

        List< Object[] > vals;
        try
        {
            vals = CSVLineParser.parseCsv((String)data);
        }
        catch (IOException e)
        {
            throw new StreamSerDeException(e);
        }
        return createAllInstance(vals);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object serialize(List< Object[] > events)
        throws StreamSerDeException
    {
        if (events == null)
        {
            LOG.info("Input event is null.");
            return null;
        }

        sb.delete(0, sb.length());
        for (Object[] event : events)
        {
            String value = csvCreator.createCSV(event);
            if (value != null)
            {
                sb.append(value + LINE_SEPARATOR_UNIX);
            }
        }

        return sb.substring(0, sb.length() - LINE_SEPARATOR_UNIX.length());

    }

}
