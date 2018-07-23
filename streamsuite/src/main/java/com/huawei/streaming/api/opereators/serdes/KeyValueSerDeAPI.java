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

package com.huawei.streaming.api.opereators.serdes;

import com.huawei.streaming.api.ConfigAnnotation;
import com.huawei.streaming.api.StreamSerDeAnnoation;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.serde.KeyValueSerDe;

/**
 * 
 * 一个很简单的，根据分隔符来确定文件序列化和反序列话的API
 * 
 * 数据的序列化和反序列化可以使用一样的参数
 * 
 */
@StreamSerDeAnnoation(KeyValueSerDe.class)
public class KeyValueSerDeAPI extends SerDeAPI
{
    
    @ConfigAnnotation(StreamingConfig.SERDE_KEYVALUESERDE_SEPARATOR)
    private String separator;

    @ConfigAnnotation(StreamingConfig.STREAMING_OPERATOR_TIMEZONE)
    private String timezoneForTimestamp;

    public String getSeparator()
    {
        return separator;
    }
    
    public void setSeparator(String separator)
    {
        this.separator = separator;
    }

    public String getTimezoneForTimestamp()
    {
        return timezoneForTimestamp;
    }

    public void setTimezoneForTimestamp(String timezoneForTimestamp)
    {
        this.timezoneForTimestamp = timezoneForTimestamp;
    }
}
