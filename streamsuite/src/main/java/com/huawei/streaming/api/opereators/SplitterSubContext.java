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
package com.huawei.streaming.api.opereators;

/**
 * split算子的每个拆分内容
 *
 */
public class SplitterSubContext
{
    private String streamName;
    
    private String outputExpression;
    
    private String filterExpression;
    
    public String getStreamName()
    {
        return streamName;
    }
    
    public void setStreamName(String streamName)
    {
        this.streamName = streamName;
    }
    
    public String getOutputExpression()
    {
        return outputExpression;
    }
    
    public void setOutputExpression(String outputExpression)
    {
        this.outputExpression = outputExpression;
    }
    
    public String getFilterExpression()
    {
        return filterExpression;
    }
    
    public void setFilterExpression(String filterExpression)
    {
        this.filterExpression = filterExpression;
    }
    
}
