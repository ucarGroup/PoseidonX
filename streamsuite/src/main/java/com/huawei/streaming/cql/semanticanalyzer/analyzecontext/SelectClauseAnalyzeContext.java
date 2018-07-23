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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * select子句语义解析内容
 * 
 */
public class SelectClauseAnalyzeContext extends ExpressionsAnalyzeContext
{
    
    /**
     * select中解析好的schema信息
     */
    private Schema outputSchema;
    
    /**
     * 是否包含distinct，
     * 即是否以distinct开头
     */
    private boolean isDistinct = false;
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParseContext(ParseContext parseContext)
    {

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
        if (isDistinct)
        {
            return "DISTINCT " + Joiner.on(", ").join(getExpdes());
        }
        else
        {
            return Joiner.on(", ").join(getExpdes());
        }
    }
    
    public boolean isDistinct()
    {
        return isDistinct;
    }
    
    public void setDistinct(boolean isdistinct)
    {
        this.isDistinct = isdistinct;
    }
    
    public void setOutputSchema(Schema outputSchema)
    {
        this.outputSchema = outputSchema;
    }
    
    public Schema getOutputSchema()
    {
        return outputSchema;
    }
    
}
