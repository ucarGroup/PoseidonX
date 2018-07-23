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

package com.huawei.streaming.cql.semanticanalyzer;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.opereators.WindowCommons;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.WindowAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.RangeBoundContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.RangeTodayContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.RangeWindowContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.RowsWindowContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.WindowDeterminerContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.WindowProperty;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.WindowSourceContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * window语义分析
 *
 */
public class WindowAnalyzer extends BaseAnalyzer
{
    private static final Logger LOG = LoggerFactory.getLogger(WindowAnalyzer.class);
    
    private static final Integer ONE_SECOND = 1000;
    
    private static final Integer ONE_MINUTE = 60 * ONE_SECOND;
    
    private static final Integer ONE_HOUR = 60 * ONE_MINUTE;
    
    private static final Integer ONE_DAY = 24 * ONE_HOUR;
    
    private WindowAnalyzeContext context;
    
    private WindowSourceContext windowParseContext;
    
    /**
     * <默认构造函数>
     *
     */
    public WindowAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        windowParseContext = (WindowSourceContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public WindowAnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        parseRangeTodayWindow();
        parseRangeWindow();
        parseRowsWindow();
        validateWindow();
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        context = new WindowAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return context;
    }
    
    private void validateWindow()
        throws SemanticAnalyzerException
    {
        /*
         * exlude不支持的窗口类型
         * S[ROWS N1 SLIDE SORT BY EXP1 {ASC|DESC} {EXCLUDE NOW} ]
         * S[RANGE T1 SLIDE {EXCLUDE NOW} ]
         * S[RANGE T1 SLIDE PARTITION BY EXP1 {EXCLUDE NOW} ]
         * S[RANGE T1 SLIDE SORT BY EXP1 {ASC|DESC} {EXCLUDE NOW} ]
         * S[ROWS N1 BATCH {EXCLUDE NOW}] 
         * S[ROWS N1 BATCH PARTITION BY EXP1 {EXCLUDE NOW} ]
         * S[RANGE T1 BATCH {EXCLUDE NOW} ]
         * S[RANGE T1 BATCH PARTITION BY EXP1 {EXCLUDE NOW} ]
         * S[RANGE T1 BATCH TRIGGER BY EXP1 {EXCLUDE NOW} ]
         * S[RANGE T1 BATCH PARTITION BY EXP1 TRIGGER BY EXP2 {EXCLUDE NOW} ]
         */
        if (!context.isExcludeNow())
        {
            return;
        }
        
        //1、所有的batch窗口都不支持exclude now
        if (context.getWindowProperty() == WindowProperty.BATCH)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_UNSUPPORTED_EXCLUDE_NOW);
            LOG.error("Unsupported exclude", exception);
            throw exception;
        }
        
        /*
         * S[ROWS N1 SLIDE SORT BY EXP1 {ASC|DESC} {EXCLUDE NOW} ]
         * S[RANGE T1 SLIDE {EXCLUDE NOW} ]
         * S[RANGE T1 SLIDE PARTITION BY EXP1 {EXCLUDE NOW} ]
         * S[RANGE T1 SLIDE SORT BY EXP1 {ASC|DESC} {EXCLUDE NOW} ]
         */
        Set<String> sets =
            Sets.newHashSet(WindowCommons.LENGTH_SORT_WINDOW,
                WindowCommons.TIME_SLIDE_WINDOW,
                WindowCommons.GROUP_TIME_SLIDE_WINDOW,
                WindowCommons.TIME_SORT_WINDOW);
        
        Window window = context.createWindowByParseContext();
        
        if (sets.contains(window.getName()))
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_UNSUPPORTED_EXCLUDE_NOW);
            LOG.error("Unsupported exclude", exception);
            throw exception;
        }
    }
    
    private void parseRowsWindow()
        throws SemanticAnalyzerException
    {
        if (windowParseContext.getRowsWindow() == null)
        {
            return;
        }
        
        RowsWindowContext rowsWindow = windowParseContext.getRowsWindow();
        context.setRows(formatLong(rowsWindow.getRows()));
        context.setWindowProperty(rowsWindow.getWindowProperty());
        analyzeWindowDeterminerContext(rowsWindow.getDeterminer());
    }
    
    private void parseRangeWindow()
        throws SemanticAnalyzerException
    {
        if (windowParseContext.getRangeWindow() == null)
        {
            return;
        }
        
        RangeWindowContext rangeWindow = windowParseContext.getRangeWindow();
        context.setWindowProperty(rangeWindow.getWindowProperty());
        analyzeRangeBound(rangeWindow.getBound());
        analyzeWindowDeterminerContext(rangeWindow.getDeterminer());
    }
    
    private void parseRangeTodayWindow()
        throws SemanticAnalyzerException
    {
        if (windowParseContext.getRangeToday() == null)
        {
            return;
        }
        
        RangeTodayContext rangeTodayWindow = windowParseContext.getRangeToday();
        ExpressionContext todayExpression = rangeTodayWindow.getTodayExpression();
        ExpressionDescribe expDesc = ExpressionDescFactory.createExpressionDesc(todayExpression, getAllSchemas());
        context.setRangeTodayExpression(expDesc.toString());
        analyzeWindowDeterminerContext(rangeTodayWindow.getDeterminer());
        
    }
    
    private void analyzeRangeBound(RangeBoundContext range)
        throws SemanticAnalyzerException
    {
        context.setUnbounded(range.isUnbounded());
        long rangeMilliSeconds = 0;
        rangeMilliSeconds = analyzeDay(range, rangeMilliSeconds);
        rangeMilliSeconds = analyzeHour(range, rangeMilliSeconds);
        rangeMilliSeconds = analyzeMinutes(range, rangeMilliSeconds);
        rangeMilliSeconds = analyzeSeconds(range, rangeMilliSeconds);
        rangeMilliSeconds = analyzeMilliSeconds(range, rangeMilliSeconds);
        context.setRange(rangeMilliSeconds);
    }
    
    private long analyzeMilliSeconds(RangeBoundContext range, long rangeMilliSeconds)
        throws SemanticAnalyzerException
    {
        if (range.getMilliseconds() != null)
        {
            rangeMilliSeconds = rangeMilliSeconds + formatLong(range.getMilliseconds());
        }
        
        return rangeMilliSeconds;
    }
    
    private long analyzeSeconds(RangeBoundContext range, long rangeMilliSeconds)
        throws SemanticAnalyzerException
    {
        if (range.getSeconds() != null)
        {
            rangeMilliSeconds = rangeMilliSeconds + ONE_SECOND * formatLong(range.getSeconds());
        }
        
        return rangeMilliSeconds;
    }
    
    private long analyzeMinutes(RangeBoundContext range, long rangeMilliSeconds)
        throws SemanticAnalyzerException
    {
        if (range.getMinutes() != null)
        {
            rangeMilliSeconds = rangeMilliSeconds + ONE_MINUTE * formatLong(range.getMinutes());
        }
        
        return rangeMilliSeconds;
    }
    
    private long analyzeHour(RangeBoundContext range, long rangeMilliSeconds)
        throws SemanticAnalyzerException
    {
        if (range.getHour() != null)
        {
            rangeMilliSeconds = rangeMilliSeconds + ONE_HOUR * formatLong(range.getHour());
        }
        
        return rangeMilliSeconds;
    }
    
    private long analyzeDay(RangeBoundContext range, long rangeMilliSeconds)
        throws SemanticAnalyzerException
    {
        if (range.getDay() != null)
        {
            rangeMilliSeconds = rangeMilliSeconds + ONE_DAY * formatLong(range.getDay());
        }
        
        return rangeMilliSeconds;
    }
    
    private void analyzeWindowDeterminerContext(WindowDeterminerContext determiner)
        throws SemanticAnalyzerException
    {
        if (determiner == null)
        {
            return;
        }
        
        analyzeWindowDeterminerPartitionBy(determiner);
        analyzeWindowDeterminerSortBy(determiner);
        analyzeWindowDeterminerTriggerBy(determiner);
        analyzeWindowDeterminerExcludeNow(determiner);
    }
    
    private void analyzeWindowDeterminerExcludeNow(WindowDeterminerContext determiner)
        throws SemanticAnalyzerException
    {
        context.setExcludeNow(determiner.isExcludeNow());
    }
    
    private void analyzeWindowDeterminerTriggerBy(WindowDeterminerContext determiner)
        throws SemanticAnalyzerException
    {
        if (determiner.getTriggerbyExpression() != null)
        {
            ExpressionContext expression = determiner.getTriggerbyExpression().getExpression();
            ExpressionDescribe expDesc = ExpressionDescFactory.createExpressionDesc(expression, getAllSchemas());
            context.setTriggerByExpression(expDesc.toString());
        }
    }
    
    private void analyzeWindowDeterminerSortBy(WindowDeterminerContext determiner)
        throws SemanticAnalyzerException
    {
        if (determiner.getSortbyDeterminer() != null)
        {
            context.setSortByExpression(determiner.getSortbyDeterminer().getExpressionString());
        }
    }
    
    private void analyzeWindowDeterminerPartitionBy(WindowDeterminerContext determiner)
        throws SemanticAnalyzerException
    {
        if (determiner.getPartitionByExpression() != null)
        {
            ExpressionContext exp = determiner.getPartitionByExpression().getExpression();
            ExpressionDescribe expDesc = ExpressionDescFactory.createExpressionDesc(exp, super.getAllSchemas());
            context.setPartitionByExpression(expDesc.toString());
        }
    }
    
    private Long formatLong(String text)
        throws SemanticAnalyzerException
    {
        try
        {
            return Long.valueOf(text);
        }
        catch (NumberFormatException e)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_CONSTANT_FORMAT, text, "LONG");
            LOG.error("Window parameter type error.", exception);
            
            throw exception;
        }
    }
    
}
