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
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * insert语句语义分析内容
 *
 */
public class InsertAnalyzeContext extends InsertOnlyAnalyzeContext
{
    
    private SelectAnalyzeContext selectContext;
    
    private ParseContext context;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParseContext(ParseContext parseContext)
    {
        context = parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<Schema> getCreatedSchemas()
    {
        List<Schema> schemas = Lists.newArrayList();
        schemas.addAll(selectContext.getCreatedSchemas());
        schemas.addAll(super.getCreatedSchemas());
        return schemas;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return context.toString();
    }
    
    public SelectAnalyzeContext getSelectContext()
    {
        return selectContext;
    }
    
    public void setSelectContext(SelectAnalyzeContext selectContext)
    {
        this.selectContext = selectContext;
    }
    
}
