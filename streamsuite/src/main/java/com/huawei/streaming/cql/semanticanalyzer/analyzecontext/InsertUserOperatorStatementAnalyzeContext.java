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
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FullUserOperatorContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * 使用自定义算子语义分析内容
 */
public class InsertUserOperatorStatementAnalyzeContext extends AnalyzeContext
{
    private String operatorClassName;
    
    private Schema inputSchmea;
    
    private Schema outputSchema;
    
    private TreeMap<String, String> properties;
    
    private String operatorName;
    
    private String inputStreamName;
    
    private String outputStreamName;
    
    private String distributedByColumnName;
    
    private Integer parallelNumber;
    
    private FullUserOperatorContext parseContext = null;
    
    private boolean isPipeStreamNotCreated = false;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParseContext(ParseContext parseContext)
    {
        this.parseContext = (FullUserOperatorContext)parseContext;
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
        List<Schema> schemas = Lists.newArrayList();
        if (isPipeStreamNotCreated)
        {
            schemas.add(outputSchema);
        }
        return schemas;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        if (parseContext == null)
        {
            return "";
        }
        
        return parseContext.toString();
    }
    
    public String getOperatorName()
    {
        return operatorName;
    }
    
    public void setOperatorName(String operatorName)
    {
        this.operatorName = operatorName;
    }
    
    public String getInputStreamName()
    {
        return inputStreamName;
    }
    
    public void setInputStreamName(String inputStreamName)
    {
        this.inputStreamName = inputStreamName;
    }
    
    public String getOutputStreamName()
    {
        return outputStreamName;
    }
    
    public void setOutputStreamName(String outputStreamName)
    {
        this.outputStreamName = outputStreamName;
    }
    
    public String getDistributedByColumnName()
    {
        return distributedByColumnName;
    }
    
    public void setDistributedByColumnName(String distributedByColumnName)
    {
        this.distributedByColumnName = distributedByColumnName;
    }
    
    public Integer getParallelNumber()
    {
        return parallelNumber;
    }
    
    public void setParallelNumber(Integer parallelNumber)
    {
        this.parallelNumber = parallelNumber;
    }
    
    public ParseContext getParseContext()
    {
        return parseContext;
    }
    
    public String getOperatorClassName()
    {
        return operatorClassName;
    }
    
    public void setOperatorClassName(String operatorClassName)
    {
        this.operatorClassName = operatorClassName;
    }
    
    public Schema getInputSchmea()
    {
        return inputSchmea;
    }
    
    public void setInputSchmea(Schema inputSchmea)
    {
        this.inputSchmea = inputSchmea;
    }
    
    public Schema getOutputSchema()
    {
        return outputSchema;
    }
    
    public void setOutputSchema(Schema outputSchema)
    {
        this.outputSchema = outputSchema;
    }
    
    public TreeMap<String, String> getProperties()
    {
        return properties;
    }
    
    public void setProperties(TreeMap<String, String> properties)
    {
        this.properties = properties;
    }
    
    public boolean isPipeStreamNotCreated()
    {
        return isPipeStreamNotCreated;
    }
    
    public void setPipeStreamNotCreated(boolean isCreated)
    {
        this.isPipeStreamNotCreated = isCreated;
    }
    
}
