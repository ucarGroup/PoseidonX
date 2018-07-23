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
import com.huawei.streaming.cql.semanticanalyzer.CreateOutputStreamAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * 创建输出流语法解析内容
 *
 */
public class CreateOutputStatementContext extends CreateStreamStatementContext
{
    private ClassNameContext serClassName;
    
    private StreamPropertiesContext serProperties;
    
    private ClassNameContext sinkClassName;
    
    private StreamPropertiesContext sinkProperties;
    
    private ParallelClauseContext parallelNumber;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("CREATE OUTPUT STREAM ");
        sb.append(getStreamName());
        sb.append(" " + getColumns().toString());
        
        if (serClassName != null)
        {
            sb.append(" SERDE " + serClassName.toString());
        }
        
        if (serProperties != null)
        {
            sb.append(" " + serProperties.toString());
        }
        
        sb.append(" SINK " + sinkClassName.toString());
        if (sinkProperties != null)
        {
            sb.append(" " + sinkProperties.toString());
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
        return new CreateOutputStreamAnalyzer(this);
    }
    
    public ClassNameContext getSerClassName()
    {
        return serClassName;
    }
    
    public void setSerClassName(ClassNameContext serClassName)
    {
        this.serClassName = serClassName;
    }
    
    public StreamPropertiesContext getSerProperties()
    {
        return serProperties;
    }
    
    public void setSerProperties(StreamPropertiesContext serProperties)
    {
        this.serProperties = serProperties;
    }
    
    public ClassNameContext getSinkClassName()
    {
        return sinkClassName;
    }
    
    public void setSinkClassName(ClassNameContext sinkClassName)
    {
        this.sinkClassName = sinkClassName;
    }
    
    public StreamPropertiesContext getSinkProperties()
    {
        return sinkProperties;
    }
    
    public void setSinkProperties(StreamPropertiesContext sinkProperties)
    {
        this.sinkProperties = sinkProperties;
    }
    
    public ParallelClauseContext getParallelNumber()
    {
        return parallelNumber;
    }
    
    public void setParallelNumber(ParallelClauseContext parallelNumber)
    {
        this.parallelNumber = parallelNumber;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        super.walkChild(walker);
        walkExpression(walker, parallelNumber);
        walkExpression(walker, serProperties);
        walkExpression(walker, sinkProperties);
    }
    
}
