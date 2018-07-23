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
import com.huawei.streaming.cql.semanticanalyzer.WindowAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.Task;

/**
 * windowsource解析内容
 * 
 */
public class WindowSourceContext extends ParseContext
{
    private RangeWindowContext rangeWindow;
    
    private RangeTodayContext rangeToday;
    
    private RowsWindowContext rowsWindow;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        if (rangeWindow != null)
        {
            sb.append(rangeWindow.toString());
        }
        if (rangeToday != null)
        {
            sb.append(rangeToday.toString());
        }
        if (rowsWindow != null)
        {
            sb.append(rowsWindow.toString());
        }
        sb.append("]");
        
        return sb.toString();
    }
    
    public RangeWindowContext getRangeWindow()
    {
        return rangeWindow;
    }
    
    public void setRangeWindow(RangeWindowContext rangeWindow)
    {
        this.rangeWindow = rangeWindow;
    }
    
    public RangeTodayContext getRangeToday()
    {
        return rangeToday;
    }
    
    public void setRangeToday(RangeTodayContext rangeToday)
    {
        this.rangeToday = rangeToday;
    }
    
    public RowsWindowContext getRowsWindow()
    {
        return rowsWindow;
    }
    
    public void setRowsWindow(RowsWindowContext rowsWindow)
    {
        this.rowsWindow = rowsWindow;
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
        return new WindowAnalyzer(this);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, rangeToday);
        walkExpression(walker, rangeWindow);
        walkExpression(walker, rowsWindow);
    }
    
}
