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

import java.util.List;

import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.Task;

/**
 * from解析内容
 * 
 */
public class FromSourceContext extends ParseContext
{
    private StreamBodyContext streamBody;
    
    private DataSourceBodyContext dataSoruceBody;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return streamBody != null ? streamBody.toString() : dataSoruceBody.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, streamBody);
        walkExpression(walker, dataSoruceBody);
    }
    
    public StreamBodyContext getStreamBody()
    {
        return streamBody;
    }
    
    public void setStreamBody(StreamBodyContext streamBody)
    {
        this.streamBody = streamBody;
    }
    
    public DataSourceBodyContext getDataSoruceBody()
    {
        return dataSoruceBody;
    }
    
    public void setDataSoruceBody(DataSourceBodyContext dataSoruceBody)
    {
        this.dataSoruceBody = dataSoruceBody;
    }
}
