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

package com.huawei.streaming.cql.semanticanalyzer.parser.context;

import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.CreateInputStreamAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * 创建输入流语法解析内容
 *
 */
public class CreateInputStatementContext extends CreateStreamStatementContext
{
    private ClassNameContext deserClassName;
    
    private StreamPropertiesContext deserProperties;
    
    private ClassNameContext sourceClassName;
    
    private StreamPropertiesContext sourceProperties;
    
    private ParallelClauseContext parallelNumber;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("CREATE INPUT STREAM ");
        sb.append(getStreamName());
        sb.append(" " + getColumns().toString());
        
        if (deserClassName != null)
        {
            sb.append(" SERDE " + deserClassName.toString());
        }
        
        if (deserProperties != null)
        {
            sb.append(" " + deserProperties.toString());
        }
        
        sb.append(" SOURCE " + sourceClassName.toString());
        if (sourceProperties != null)
        {
            sb.append(" " + sourceProperties.toString());
        }
        
        if (parallelNumber != null)
        {
            sb.append(" " + parallelNumber.toString());
        }
        
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException
    {
        return new CreateInputStreamAnalyzer(this);
    }
    
    public ClassNameContext getSourceClassName()
    {
        return sourceClassName;
    }
    
    public void setSourceClassName(ClassNameContext sourceClassName)
    {
        this.sourceClassName = sourceClassName;
    }
    
    public StreamPropertiesContext getSourceProperties()
    {
        return sourceProperties;
    }
    
    public void setSourceProperties(StreamPropertiesContext sourceProperties)
    {
        this.sourceProperties = sourceProperties;
    }
    
    public ParallelClauseContext getParallelNumber()
    {
        return parallelNumber;
    }
    
    public void setParallelNumber(ParallelClauseContext parallelNumber)
    {
        this.parallelNumber = parallelNumber;
    }
    
    public ClassNameContext getDeserClassName()
    {
        return deserClassName;
    }
    
    public void setDeserClassName(ClassNameContext deserClassName)
    {
        this.deserClassName = deserClassName;
    }
    
    public StreamPropertiesContext getDeserProperties()
    {
        return deserProperties;
    }
    
    public void setDeserProperties(StreamPropertiesContext deserProperties)
    {
        this.deserProperties = deserProperties;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        super.walkChild(walker);
        walkExpression(walker, deserProperties);
        walkExpression(walker, parallelNumber);
        walkExpression(walker, sourceProperties);
    }
}
