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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc;

/**
 * 流名称以及别名描述信息
 * 
 */
public class StreamAliasDesc implements ExpressionDescribe
{
    private String streamName;
    
    private String streamAlias;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        if (streamAlias == null)
        {
            return " " + streamName;
        }
        return " " + streamName + " as " + streamAlias + " ";
    }
    
    public String getStreamName()
    {
        return streamName;
    }
    
    public void setStreamName(String streamName)
    {
        this.streamName = streamName;
    }
    
    public String getStreamAlias()
    {
        return streamAlias;
    }
    
    public void setStreamAlias(String streamAlias)
    {
        this.streamAlias = streamAlias;
    }
    
}
