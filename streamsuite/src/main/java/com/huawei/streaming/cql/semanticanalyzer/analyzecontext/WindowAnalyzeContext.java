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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.opereators.WindowCommons;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.WindowProperty;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.WindowSourceContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * window语义分析内容
 *
 */
public class WindowAnalyzeContext extends AnalyzeContext
{
    private static final Logger LOG = LoggerFactory.getLogger(WindowAnalyzeContext.class);
    
    private WindowSourceContext context;
    
    private long rows;
    
    private long range;
    
    private boolean isUnbounded;
    
    private boolean isExcludeNow = false;
    
    private String rangeTodayExpression;
    
    private String sortByExpression;
    
    private String partitionByExpression;
    
    private String triggerByExpression;
    
    private WindowProperty windowProperty;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParseContext(ParseContext parseContext)
    {
        context = (WindowSourceContext)parseContext;
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
        return context.toString();
    }
    
    /**
     * 创建窗口实例
     *
     */
    public Window createWindowByParseContext()
        throws SemanticAnalyzerException
    {
        Window window = Window.createKeepAllWindow();
        window.setGroupbyExpression(partitionByExpression);
        window.setExcludeNow(isExcludeNow);
        window.setOrderbyExpression(sortByExpression);
        window.setTimestampField(triggerByExpression);
        //如果不是事件驱动，就试试看是不是自然天
        if (window.getTimestampField() == null)
        {
            window.setTimestampField(rangeTodayExpression);
        }
        
        if (this.rows != 0)
        {
            window.setLength(this.rows);
        }
        //如果不是事件驱动，就试试看是不是自然天
        if (this.range != 0)
        {
            window.setLength(this.range);
        }
        
        window.setName(getWindowName());
        return window;
    }
    
    public long getRows()
    {
        return rows;
    }
    
    public void setRows(long rows)
    {
        this.rows = rows;
    }
    
    public long getRange()
    {
        return range;
    }
    
    public void setRange(long range)
    {
        this.range = range;
    }
    
    public boolean isUnbounded()
    {
        return isUnbounded;
    }
    
    public void setUnbounded(boolean isunbounded)
    {
        this.isUnbounded = isunbounded;
    }
    
    public boolean isExcludeNow()
    {
        return isExcludeNow;
    }
    
    public void setExcludeNow(boolean isexcludeNow)
    {
        this.isExcludeNow = isexcludeNow;
    }
    
    public String getRangeTodayExpression()
    {
        return rangeTodayExpression;
    }
    
    public void setRangeTodayExpression(String rangeTodayExpression)
    {
        this.rangeTodayExpression = rangeTodayExpression;
    }
    
    public String getSortByExpression()
    {
        return sortByExpression;
    }
    
    public void setSortByExpression(String sortByExpression)
    {
        this.sortByExpression = sortByExpression;
    }
    
    public String getPartitionByExpression()
    {
        return partitionByExpression;
    }
    
    public void setPartitionByExpression(String partitionByExpression)
    {
        this.partitionByExpression = partitionByExpression;
    }
    
    public String getTriggerByExpression()
    {
        return triggerByExpression;
    }
    
    public void setTriggerByExpression(String triggerByExpression)
    {
        this.triggerByExpression = triggerByExpression;
    }
    
    public WindowProperty getWindowProperty()
    {
        return windowProperty;
    }
    
    public void setWindowProperty(WindowProperty windowProperty)
    {
        this.windowProperty = windowProperty;
    }
    
    private String getWindowName()
        throws SemanticAnalyzerException
    {
        String windowName = null;
        windowName = recognizeKeepAllWindow();
        if (windowName != null)
        {
            return windowName;
        }
        windowName = recognizeEventWindows();
        if (windowName != null)
        {
            return windowName;
        }
        windowName = recognizeNaturalDayWindows();
        if (windowName != null)
        {
            return windowName;
        }
        windowName = recognizeSortWindows();
        if (windowName != null)
        {
            return windowName;
        }
        windowName = recognizeRangeWindows();
        if (windowName != null)
        {
            return windowName;
        }
        windowName = recognizeRowWindows();
        if (windowName != null)
        {
            return windowName;
        }
        
        SemanticAnalyzerException exception =
            new SemanticAnalyzerException(ErrorCode.WINDOW_UNRECGNIZE_WINDOW, this.toString());
        LOG.error("Window define error.", exception);
        
        throw exception;
    }
    
    private String recognizeKeepAllWindow()
    {
        if (this.isUnbounded)
        {
            return WindowCommons.KEEPALL_WINDOW;
        }
        return null;
    }
    
    private String recognizeEventWindows()
    {
        if (this.triggerByExpression != null)
        {
            return parseTriggerByWindow();
        }
        return null;
    }
    
    private String parseTriggerByWindow()
    {
        if (this.partitionByExpression != null)
        {
            return recognizeGroupedEventWindow();
        }
        else
        {
            return recognizeEventWindow();
        }
    }
    
    private String recognizeEventWindow()
    {
        if (this.windowProperty == WindowProperty.BATCH)
        {
            return WindowCommons.EVENT_TBATCH_WINDOW;
        }
        else
        {
            return WindowCommons.EVENT_TSLIDE_WINDOW;
        }
    }
    
    private String recognizeGroupedEventWindow()
    {
        if (this.windowProperty == WindowProperty.BATCH)
        {
            return WindowCommons.GROUP_EVENT_TBATCH_WINDOW;
        }
        else
        {
            return WindowCommons.GROUP_EVENT_TSLIDE_WINDOW;
        }
    }
    
    private String recognizeNaturalDayWindows()
    {
        if (this.rangeTodayExpression != null)
        {
            return parseRangeTodayWindow();
        }
        return null;
    }
    
    private String parseRangeTodayWindow()
    {
        if (this.partitionByExpression != null)
        {
            return WindowCommons.GROUP_TODAY_WINDOW;
        }
        else
        {
            return WindowCommons.TODAY_WINDOW;
        }
    }
    
    private String recognizeSortWindows()
        throws SemanticAnalyzerException
    {
        //sort窗口不支持batch
        if (this.sortByExpression != null)
        {
            if (this.range != 0)
            {
                if(this.windowProperty == WindowProperty.BATCH)
                {
                    SemanticAnalyzerException exception =
                        new SemanticAnalyzerException(ErrorCode.WINDOW_SLIDEONLY_SORTWINDOW);
                    LOG.error("Only slide type is allowed in sort window.", exception);
                    throw exception;
                }
                return WindowCommons.TIME_SORT_WINDOW;
            }
            
            if (this.rows != 0)
            {
                if(this.windowProperty == WindowProperty.BATCH)
                {
                    SemanticAnalyzerException exception =
                        new SemanticAnalyzerException(ErrorCode.WINDOW_SLIDEONLY_SORTWINDOW);
                    LOG.error("Only slide type is allowed in sort window.", exception);
                    throw exception;
                }
                
                return WindowCommons.LENGTH_SORT_WINDOW;
            }
            
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.WINDOW_UNRECGNIZE_WINDOW, this.toString());
            LOG.error("Unsupport window type.", exception);
            
            throw exception;
        }
        return null;
    }
    
    private String recognizeRangeWindows()
    {
        if (this.range != 0)
        {
            if (this.partitionByExpression != null)
            {
                return recognizeGroupedTimeWindow();
            }
            else
            {
                return recognizeTimeWindow();
            }
        }
        return null;
    }
    
    private String recognizeTimeWindow()
    {
        if (this.windowProperty == WindowProperty.BATCH )
        {
            return WindowCommons.TIME_BATCH_WINDOW;
        }
        else if(this.windowProperty == WindowProperty.ACCUMBATCH)
        {
            return WindowCommons.TIME_ACCUMBATCH_WINDOW;
        }
        else
        {
            return WindowCommons.TIME_SLIDE_WINDOW;
        }
    }
    
    private String recognizeGroupedTimeWindow()
    {
        if (this.windowProperty == WindowProperty.BATCH)
        {
            return WindowCommons.GROUP_TIME_BATCH_WINDOW;
        }
        else
        {
            return WindowCommons.GROUP_TIME_SLIDE_WINDOW;
        }
    }
    
    private String recognizeRowWindows()
    {
        if (this.rows != 0)
        {
            if (this.partitionByExpression != null)
            {
                if (this.windowProperty == WindowProperty.BATCH)
                {
                    return WindowCommons.GROUP_LENGTH_BATCH_WINDOW;
                }
                else
                {
                    return WindowCommons.GROUP_LENGTH_SLIDE_WINDOW;
                }
            }
            else
            {
                if (this.windowProperty == WindowProperty.BATCH)
                {
                    return WindowCommons.LENGTH_BATCH_WINDOW;
                }
                else
                {
                    return WindowCommons.LENGTH_SLIDE_WINDOW;
                }
            }
        }
        return null;
    }
    
}
