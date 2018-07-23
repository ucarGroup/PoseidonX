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
 * 范围窗口解析内容
 * 
 */
public class RangeBoundContext extends ParseContext
{
    
    private boolean isUnbounded = false;
    
    private String day;
    
    private String hour;
    
    private String minutes;
    
    private String seconds;
    
    private String milliseconds;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        if (isUnbounded)
        {
            return " UNBOUNDED";
        }
        
        StringBuilder sb = new StringBuilder();
        
        if (day != null)
        {
            sb.append(" " + day + " DAYS");
        }
        
        if (hour != null)
        {
            sb.append(" " + hour + " HOURS");
        }
        
        if (minutes != null)
        {
            sb.append(" " + minutes + " MINUTES");
        }
        
        if (seconds != null)
        {
            sb.append(" " + seconds + " SECONDS");
        }
        
        if (milliseconds != null)
        {
            sb.append(" " + milliseconds + " MILLISECONDS");
        }
        
        return sb.toString();
    }
    
    public String getDay()
    {
        return day;
    }
    
    public void setDay(String day)
    {
        this.day = day;
    }
    
    public String getHour()
    {
        return hour;
    }
    
    public void setHour(String hour)
    {
        this.hour = hour;
    }
    
    public String getMinutes()
    {
        return minutes;
    }
    
    public void setMinutes(String minutes)
    {
        this.minutes = minutes;
    }
    
    public String getSeconds()
    {
        return seconds;
    }
    
    public void setSeconds(String seconds)
    {
        this.seconds = seconds;
    }
    
    public String getMilliseconds()
    {
        return milliseconds;
    }
    
    public void setMilliseconds(String milliseconds)
    {
        this.milliseconds = milliseconds;
    }
    
    public boolean isUnbounded()
    {
        return isUnbounded;
    }
    
    public void setUnbounded(boolean isunbounded)
    {
        this.isUnbounded = isunbounded;
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
        
    }
}
