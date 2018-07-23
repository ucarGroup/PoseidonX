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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext;

import java.util.List;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.DropApplicationContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * 删除应用程序语义分析结果
 * 
 */
public class DropApplicationAnalyzeContext extends AnalyzeContext
{
    private DropApplicationContext context;
    
    private boolean isCheckExist = false;
    
    private String appName;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParseContext(ParseContext parseContext)
    {
        context = (DropApplicationContext)parseContext;
        isCheckExist = context.isIfExists();
        appName = context.getApplicationName();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void validateParseContext()
        throws SemanticAnalyzerException
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<Schema> getCreatedSchemas()
    {
        return Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "";
    }
    
    public boolean isCheckExist()
    {
        return isCheckExist;
    }
    
    public void setCheckExist(boolean ischeckExist)
    {
        this.isCheckExist = ischeckExist;
    }
    
    public String getAppName()
    {
        return appName;
    }
    
    public void setAppName(String appName)
    {
        this.appName = appName;
    }
}
