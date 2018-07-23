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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.StreamingUtils;

/**
 * 最简单的数据序列化和反序列化规则
 * 
 * 使用配置的分隔符拆分消息
 * 
 */
public class SimpleSerDe extends BaseSerDe
{
    private static final Logger LOG = LoggerFactory.getLogger(SimpleSerDe.class);
    
    private static final long serialVersionUID = -2364817027725796314L;
    
    private List<Object[]> nullResults = Lists.newArrayList();

    public static final String LINE_SEPARATOR_UNIX = "\n";

    private String separator = ",";


    private StringBuilder sb = new StringBuilder();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf)
        throws StreamingException
    {
        super.setConfig(conf);
        separator = getConfig().getStringValue(StreamingConfig.SERDE_SIMPLESERDE_SEPARATOR);
    }

    /**
     * {@inheritDoc}
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
        
        String sData = data.toString();
        //空字符串当作null处理，这样才可以保证is null判断的正确性
        if (Strings.isNullOrEmpty(sData))
        {
            LOG.debug("Input raw data is null.");
            return nullResults;
        }
        
        List<Object[]> splitResults = Lists.newArrayList();
        Object[] values = StreamingUtils.splitByWholeSeparator(sData, separator, -1, true);
        splitResults.add(values);
        return createAllInstance(splitResults);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object serialize(List<Object[]> event)
        throws StreamSerDeException
    {
        if (event == null)
        {
            LOG.info("Input event is null.");
            return null;
        }

        sb.delete(0, sb.length());
        for (int i = 0; i < event.size(); i++)
        {
            Object[] vals = event.get(i);
            String result = lineSerialize(vals);
            if (result != null)
            {
                sb.append(result + LINE_SEPARATOR_UNIX);
            }
        }

        return sb.substring(0, sb.length() - LINE_SEPARATOR_UNIX.length());
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
            lineSb.append(result[i]);
            if (i != result.length - 1)
            {
                lineSb.append(separator);
            }
        }
        return lineSb.toString();
    }
}
