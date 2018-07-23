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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.StreamingUtils;

/**
 * Key value 格式数据的反序列化
 * 
 */
public class KeyValueSerDe extends BaseSerDe
{
    private static final long serialVersionUID = 522899593665187443L;
    
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueSerDe.class);

    public static final String LINE_SEPARATOR_UNIX = "\n";

    private static final int KEYVALUE_ARRAY_LENGTH = 2;

    private String separator = ",";
    
    private List<Object[]> nullResults = Lists.newArrayList();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf)
        throws StreamingException
    {
        super.setConfig(conf);
        separator = getConfig().getStringValue(StreamingConfig.SERDE_KEYVALUESERDE_SEPARATOR);
    }
    
    /**
     * 将原始数据按照设定格式分解
     * 
     * 原始传过来的data数据中的kv顺序可能是乱的，所以需要根据key的名称纠正顺序
     * 
     */
    @Override
    public List<Object[]> deSerialize(Object data)
        throws StreamSerDeException
    {
        if (data == null)
        {
            LOG.debug("Input raw data is null.");
            return nullResults;
        }
        
        List<Object[]> values = getOrderedValues((String)data);
        return createAllInstance(values);
    }
    
    /** {@inheritDoc} */
    @Override
    public Object serialize(List<Object[]> event)
        throws StreamSerDeException
    {
        if (event == null)
        {
            LOG.info("Input event is null.");
            return null;
        }
        //get the keys
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < event.size(); i++)
        {
            Object[] vals = event.get(i);
            String lineResult = lineSerialize(vals);
            if (lineResult != null)
            {
                sb.append(lineResult + LINE_SEPARATOR_UNIX);
            }
        }
        return sb.length() == 0 ? null : sb.substring(0, sb.length() - LINE_SEPARATOR_UNIX.length());
    }

    private String lineSerialize(Object[] vals)
    {
        String[] result = null;
        try
        {
            result = serializeRowToString(vals);
        }
        catch (StreamSerDeException e)
        {
            LOG.warn("One line is ignore.");
            return null;
        }

        StringBuilder lineSb = new StringBuilder();
        for (int i = 0; i < result.length; i++)
        {
            lineSb.append(getSchema().getAttributeName(i) + "=" +result[i]);
            if (i != result.length - 1)
            {
                lineSb.append(separator);
            }
        }
        return lineSb.toString();
    }

    
    private List<Object[]> getOrderedValues(String data)
        throws StreamSerDeException
    {
        List<Object[]> splitResults = Lists.newArrayList();
        
        String[] kvs = splitKVs(data);
        Map<String, String> keyValues = splitKeyValueMaps(kvs);
        
        String[] values = new String[kvs.length];
        
        for (int i = 0; i < kvs.length; i++)
        {
            values[i] = keyValues.get(getSchema().getAllAttributeNames()[i]);
        }
        splitResults.add(values);
        return splitResults;
    }
    
    private Map<String, String> splitKeyValueMaps(String[] kvs)
        throws StreamSerDeException
    {
        Map<String, String> keyValues = Maps.newHashMap();
        
        for (int i = 0; i < kvs.length; i++)
        {
            String[] kvarr = kvs[i].split("=");
            if (kvarr.length != KEYVALUE_ARRAY_LENGTH)
            {
                LOG.warn("Data is not key value type.");
                throw new StreamSerDeException("Data is not key value type.");
            }
            keyValues.put(kvarr[0], kvarr[1]);
        }
        return keyValues;
    }
    
    private String[] splitKVs(String data)
        throws StreamSerDeException
    {
        String[] kvs = StreamingUtils.splitByWholeSeparator(data, separator, -1, true);
        
        if (kvs.length != getSchema().getAllAttributeNames().length)
        {
            LOG.warn("Key size doesn't equals schema columns size. key size :{} schema columns size :{}.",
                kvs.length,
                getSchema().getAllAttributeNames().length);
            throw new StreamSerDeException("Key size doesn't equals schema columns size. key size :" + kvs.length
                + " schema columns size :" + getSchema().getAllAttributeNames().length);
        }
        return kvs;
    }
}
