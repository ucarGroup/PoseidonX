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
public class StreamBodyContext extends ParseContext
{
    private StreamSourceContext streamSource;
    
    private FilterBeforeWindowContext filterBeforeWindow;
    
    private WindowSourceContext window;
    
    private String alias;
    
    private boolean isUnidirection = false;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append(streamSource.toString());
        if (filterBeforeWindow != null)
        {
            sb.append(filterBeforeWindow.toString());
        }
        if (window != null)
        {
            sb.append(window.toString());
        }
        if (alias != null)
        {
            sb.append(" AS " + alias);
        }
        
        if (isUnidirection)
        {
            sb.append(" UNIDIRECTION");
        }
        
        return sb.toString();
    }
    
    public StreamSourceContext getStreamSource()
    {
        return streamSource;
    }
    
    public void setStreamSource(StreamSourceContext streamSource)
    {
        this.streamSource = streamSource;
    }
    
    public boolean isUnidirection()
    {
        return isUnidirection;
    }
    
    public void setUnidirection(boolean isunidirection)
    {
        this.isUnidirection = isunidirection;
    }
    
    public String getAlias()
    {
        return alias;
    }
    
    public void setAlias(String alias)
    {
        this.alias = alias;
    }
    
    public FilterBeforeWindowContext getFilterBeforeWindow()
    {
        return filterBeforeWindow;
    }
    
    public void setFilterBeforeWindow(FilterBeforeWindowContext filterBeforeWindow)
    {
        this.filterBeforeWindow = filterBeforeWindow;
    }
    
    public WindowSourceContext getWindow()
    {
        return window;
    }
    
    public void setWindow(WindowSourceContext window)
    {
        this.window = window;
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
        walkExpression(walker, streamSource);
        walkExpression(walker, filterBeforeWindow);
        walkExpression(walker, window);
    }
}
