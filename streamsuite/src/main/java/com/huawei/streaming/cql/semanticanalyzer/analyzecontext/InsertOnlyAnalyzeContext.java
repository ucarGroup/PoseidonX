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

/**
 * insert语句语义分析内容
 * 
 */
public abstract class InsertOnlyAnalyzeContext extends AnalyzeContext
{
    private String outputStreamName;
    
    private Schema outputSchema;
    
    private boolean isPipeStreamNotCreated = false;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void validateParseContext()
        throws SemanticAnalyzerException
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<Schema> getCreatedSchemas()
    {
        List<Schema> schemas = Lists.newArrayList();
        if (isPipeStreamNotCreated)
        {
            schemas.add(outputSchema);
        }
        return schemas;
    }
    
    public String getOutputStreamName()
    {
        return outputStreamName;
    }
    
    public void setOutputStreamName(String outputStreamName)
    {
        this.outputStreamName = outputStreamName;
    }
    
    public Schema getOutputSchema()
    {
        return outputSchema;
    }
    
    public void setOutputSchema(Schema outputSchema)
    {
        this.outputSchema = outputSchema;
    }
    
    public boolean isPipeStreamNotCreated()
    {
        return isPipeStreamNotCreated;
    }
    
    public void setPipeStreamNotCreated(boolean ispipeStreamNotCreated)
    {
        this.isPipeStreamNotCreated = ispipeStreamNotCreated;
    }
    
}
