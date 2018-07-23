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

package com.huawei.streaming.cql.executor.operatorviewscreater;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.opereators.BasicAggFunctionOperator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionGetterStrategy;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionsWalker;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.expression.AggregateExpression;
import com.huawei.streaming.expression.AggregateGroupedExpression;
import com.huawei.streaming.expression.ConstExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.PreviousExpression;
import com.huawei.streaming.expression.PropertyValueExpression;
import com.huawei.streaming.process.GroupBySubProcess;
import com.huawei.streaming.process.LimitProcess;
import com.huawei.streaming.process.OrderBySubProcess;
import com.huawei.streaming.process.SelectSubProcess;
import com.huawei.streaming.process.agg.compute.IAggregationService;
import com.huawei.streaming.process.agg.resultmerge.AggResultSetMerge;
import com.huawei.streaming.process.agg.resultmerge.AggResultSetMergeGrouped;
import com.huawei.streaming.process.agg.resultmerge.AggResultSetMergeOnly;
import com.huawei.streaming.process.agg.resultmerge.AggResultSetMergeOnlyExclude;
import com.huawei.streaming.process.agg.resultmerge.AggResultSetMergeOnlyGrouped;
import com.huawei.streaming.process.agg.resultmerge.AggResultSetMergeOnlyGroupedExclude;
import com.huawei.streaming.process.agg.resultmerge.IAggResultSetMerge;
import com.huawei.streaming.process.sort.SortCondition;
import com.huawei.streaming.window.IWindow;
import com.huawei.streaming.window.sort.LengthSortWindow;
import com.huawei.streaming.window.sort.TimeSortWindow;

/**
 * Aggregate Service 以及result set Merge 实例创建
 * <p/>
 * ResultSetMergeImpl:
 * AggResultSetMerge:uaf(col) or udad(col,expression)，可以混合其他列
 * AggResultSetMergeOnly: 只有udaf函数，没有别的独立查询的列
 * AggResultSetMergeOnlyExclude: 只有udaf函数，没有其他独立查询列，而且排除新事件，没有不带only的，否则就没办法输出。
 * AggResultSetMergeGrouped: uaf(col) or udad(col,expression)，可以混合其他列，带goupby
 * AggResultSetMergeOnlyGrouped：只有udaf函数，没有别的独立查询的列,或者只包含groupby的表达式，
 * 这里的group by，只能是a,或者udf(a)，也允许udf(a+b),udf(a,b)
 * AggResultSetMergeOnlyGroupedExclued：只有udaf函数，没有其他独立查询列，而且排除新事件，没有不带only的，否则就没办法输出，
 * 带group by，但是不允许出现udf函数或者独立的表达式。
 *
 */
public class AggResultSetMergeViewCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(AggResultSetMergeViewCreator.class);
    
    private BasicAggFunctionOperator basicAggOperator;
    
    private EventTypeMng streamschema;
    
    private List<Schema> inputSchemas;
    
    private List<Schema> outputSchemas;
    
    private OperatorTransition transitionOut;
    
    private Map<String, IWindow> streamWindows;
    
    private IExpression expressionBeforeAggregate;
    
    private Map<String, String> systemConfig;
    
    private List<Window> operatorWindows;
    
    private boolean isGroupbyOnly = true;
    
    private boolean isUDAFOnly = false;
    
    private boolean isExcludeNow = false;
    
    private static class SingleExpressionGetterStrategy implements ExpressionGetterStrategy
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isEqual(IExpression exp)
        {
            if (exp instanceof AggregateExpression)
            {
                return true;
            }
            
            if (exp instanceof AggregateGroupedExpression)
            {
                return true;
            }
            
            if (exp instanceof PropertyValueExpression)
            {
                return true;
            }
            
            if (exp instanceof ConstExpression)
            {
                return true;
            }
            
            return false;
        }
    }
    
    private static class PVAndAggExpressionGetterStrategy implements ExpressionGetterStrategy
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isEqual(IExpression exp)
        {
            if (exp instanceof AggregateExpression)
            {
                return true;
            }
            
            if (exp instanceof PropertyValueExpression)
            {
                return true;
            }
            
            return false;
        }
    }
    
    private static class IsPreviousExpressionGetterStrategy implements ExpressionGetterStrategy
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isEqual(IExpression exp)
        {
            if (exp instanceof PreviousExpression)
            {
                return true;
            }
            return false;
        }
    }
    


    /**
     * <默认构造函数>
     *
     */
    public AggResultSetMergeViewCreator(AggResultSetParameters pars)
        throws ExecutorException
    {
        initParameters(pars);
        
        if (operatorWindows != null)
        {
            Boolean firstValue = null;
            for (Window w : operatorWindows)
            {
                if (w == null)
                {
                    continue;
                }
                if (firstValue == null)
                {
                    firstValue = w.isExcludeNow();
                }
                else
                {
                    checkExcludeArguments(firstValue, w);
                }
            }
            
            if (firstValue != null)
            {
                isExcludeNow = firstValue;
            }
        }
        
    }
    
    private void initParameters(AggResultSetParameters pars)
    {
        basicAggOperator = pars.getBasicAggOperator();
        streamschema = pars.getStreamschema();
        inputSchemas = pars.getInputSchemas();
        outputSchemas = pars.getOutputSchemas();
        transitionOut = pars.getTransitionOut();
        streamWindows = pars.getStreamWindows();
        expressionBeforeAggregate = pars.getExpressionBeforeAggregate();
        systemConfig = pars.getSystemConfig();
        operatorWindows = pars.getOperatorWindows();
    }
    
    private void checkExcludeArguments(Boolean firstValue, Window w)
        throws ExecutorException
    {
        if (!firstValue.equals(w.isExcludeNow()))
        {
            LOG.error("'EXCLUDE NOW' argument must be set in all windows in one stream.");
            throw new ExecutorException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
        }
    }
    
    /**
     * 创建aggregateResultSetMergeView
     *
     */
    public IAggResultSetMerge create()
        throws ExecutorException
    {
        SelectSubProcess selectProcessor = createSelectProcessor();
        previousProcess(selectProcessor.getExprs(), expressionBeforeAggregate, streamWindows);
        
        if (basicAggOperator.getGroupbyExpression() != null)
        {
            isGroupbyOnly = isGroupbyColsOnly(selectProcessor.getExprs());
        }
        else
        {
            isUDAFOnly = isUDAFOnly(selectProcessor.getExprs());
        }
        IAggResultSetMerge resultSetMerge = createResultSetMerge(selectProcessor);
        return resultSetMerge;
    }
    
    private IAggResultSetMerge createResultSetMerge(SelectSubProcess jselet)
        throws ExecutorException
    {
        GroupBySubProcess groupBySubProcess = createGrouopbyProcess();
        OrderBySubProcess orderBySubProcess = createOrderbyProcess();
        boolean isGroupby = groupBySubProcess != null;
        LimitProcess limitProcess = createLimitProcessor();
        
        IAggregationService aggregateService = new AggregateServiceViewCreator().create(jselet, isGroupby);
        return createResultSetMerge(jselet,
            groupBySubProcess,
            orderBySubProcess,
            isGroupby,
            limitProcess,
            aggregateService);
    }
    
    private IAggResultSetMerge createResultSetMerge(SelectSubProcess jselet, GroupBySubProcess groupBySubProcess,
        OrderBySubProcess orderBySubProcess, boolean isGroupby, LimitProcess limitProcess,
        IAggregationService aggregateService)
        throws ExecutorException
    {
        if (isExcludeNow)
        {
            if (isGroupby)
            {
                if (isGroupbyOnly)
                {
                    return new AggResultSetMergeOnlyGroupedExclude(aggregateService, jselet, groupBySubProcess,
                        orderBySubProcess, limitProcess);
                }
                ExecutorException exception = new ExecutorException(ErrorCode.WINDOW_EXCLUDE_GROUPONLY);
                LOG.error("All columns in select clause must be in group by clause with exclude now window.", exception);
                throw exception;
                
            }
            
            if (isUDAFOnly)
            {
                return new AggResultSetMergeOnlyExclude(aggregateService, jselet, groupBySubProcess, orderBySubProcess,
                    limitProcess);
            }
            ExecutorException exception = new ExecutorException(ErrorCode.WINDOW_EXCLUDE_GROUPONLY);
            LOG.error("All columns in select clause must be aggregate expression with exclude now window.", exception);
            throw exception;
            
        }
        
        if (isGroupby)
        {
            if (isGroupbyOnly)
            {
                return new AggResultSetMergeOnlyGrouped(aggregateService, jselet, groupBySubProcess, orderBySubProcess,
                    limitProcess);
            }
            return new AggResultSetMergeGrouped(aggregateService, jselet, groupBySubProcess, orderBySubProcess,
                limitProcess);
        }
        
        if (isUDAFOnly)
        {
            return new AggResultSetMergeOnly(aggregateService, jselet, groupBySubProcess, orderBySubProcess,
                limitProcess);
        }
        return new AggResultSetMerge(aggregateService, jselet, groupBySubProcess, orderBySubProcess, limitProcess);
    }
    
    private boolean isUDAFOnly(IExpression[] select)
        throws ExecutorException
    {
        List<IExpression> allExp = Lists.newArrayList();
        allExp.addAll(Arrays.asList(select));
        
        List<IExpression> allpvExpressions = getSingleExpressions(allExp);
        
        for (IExpression exp : allpvExpressions)
        {
            if (exp instanceof PropertyValueExpression)
            {
                return false;
            }
            
            if (exp instanceof ConstExpression)
            {
                return false;
            }
        }
        return true;
    }
    
    /**
     * 获取所有独立的表达式
     * 聚合也算，但是count(a+b) 只能算一个聚合表达式
     *
     */
    private List<IExpression> getSingleExpressions(List<IExpression> expressions)
        throws ExecutorException
    {
        List<IExpression> expressionContainer = Lists.newArrayList();
        
        ExpressionsWalker getter = new ExpressionsWalker(new SingleExpressionGetterStrategy());
        
        for (IExpression exp : expressions)
        {
            getter.found(exp, expressionContainer);
        }
        return expressionContainer;
    }
    
    /**
     * 判断在其他非聚合表达式中，是否只包含groupby的列
     * 比如group by  a,b
     * 允许出现 a,b,sum(c),或者 a+b,sum(c)
     * 不允许出现a,sum(c) 或者a+b+c,sum(c);
     *
     */
    private boolean isGroupbyColsOnly(IExpression[] select)
        throws ExecutorException
    {
        List<IExpression> allExp = Lists.newArrayList();
        allExp.addAll(Arrays.asList(select));
        
        List<IExpression> allpvExpressions = getPVAndAggExpressions(allExp);
        
        List<Pair<String, Integer>> noAggregateProperties = parsePVExpressions(allpvExpressions);
        
        GroupBySubProcess groupBySubProcess = createGrouopbyProcess();
        List<IExpression> groupPyExpressions = Lists.newArrayList();
        if (groupBySubProcess != null)
        {
            groupPyExpressions = getPVAndAggExpressions(Arrays.asList(groupBySubProcess.getGroupKeyExprs()));
        }
        List<Pair<String, Integer>> groupbyProperties = parsePVExpressions(groupPyExpressions);
        
        /*
         * 检查是否包含非group by列
         */
        for (Pair<String, Integer> p : noAggregateProperties)
        {
            String propertyName = p.getFirst();
            Integer schemaIndex = p.getSecond();
            boolean isOnly = false;
            for (Pair<String, Integer> gp : groupbyProperties)
            {
                if (gp.getFirst().equals(propertyName))
                {
                    if (gp.getSecond().equals(schemaIndex))
                    {
                        isOnly = true;
                    }
                }
            }
            
            if (isOnly == false)
            {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 从聚合和属性表达式中找出非聚合表达式
     *
     */
    private List<Pair<String, Integer>> parsePVExpressions(List<IExpression> allpvExpressions)
    {
        List<Pair<String, Integer>> noAggregateProperties = Lists.newArrayList();
        for (IExpression exp : allpvExpressions)
        {
            if (exp instanceof PropertyValueExpression)
            {
                PropertyValueExpression pexp = (PropertyValueExpression)exp;
                /*
                    还必须检查该属性表达式是不是group by表达式
                 */
                
                noAggregateProperties.add(new Pair<String, Integer>(pexp.getPropertyName(), pexp.getStreamIndex()));
            }
        }
        return noAggregateProperties;
    }
    
    private List<IExpression> getPVAndAggExpressions(List<IExpression> exps)
        throws ExecutorException
    {
        List<IExpression> expressionContainer = new ArrayList<IExpression>();
        
        ExpressionsWalker getter = new ExpressionsWalker(new PVAndAggExpressionGetterStrategy());
        for (IExpression exp : exps)
        {
            getter.found(exp, expressionContainer);
        }
        return expressionContainer;
    }
    
    private LimitProcess createLimitProcessor()
        throws ExecutorException
    {
        if (basicAggOperator.getLimit() != null)
        {
            return new LimitViewCreator().create(basicAggOperator.getLimit());
        }
        return null;
    }
    
    private OrderBySubProcess createOrderbyProcess()
        throws ExecutorException
    {
        List<SortCondition> sortConditions =
            new OrderByViewCreator().create(this.outputSchemas, basicAggOperator.getOrderBy());
        if (sortConditions == null || sortConditions.size() == 0)
        {
            return null;
        }
        return new OrderBySubProcess(sortConditions);
    }
    
    private GroupBySubProcess createGrouopbyProcess()
        throws ExecutorException
    {
        /*
         * 无需移除groupby中的流名称，后面还是要做解析的，
         * 而且移除之后，没办法做join的group by了
         */
        String groupbyExpression = basicAggOperator.getGroupbyExpression();
        IExpression[] groupKeyExprs = new GroupByViewCreator().create(inputSchemas, groupbyExpression, systemConfig);
        if (null == groupKeyExprs || groupKeyExprs.length < 1)
        {
            return null;
        }
        return new GroupBySubProcess(groupKeyExprs);
        
    }
    
    /**
     * previous表达式处理
     * 1、检查是否包含previous表达式
     * 2、为每个包含previous表达式的流创建service
     * 3、为每个previous表达式设置对应流的service
     *
     */
    private void previousProcess(IExpression[] select, IExpression where, Map< String, IWindow > streamWindows)
        throws ExecutorException
    {
        Map<String, List<PreviousExpression>> previous = getPrevious(select, where);
        if (previous.size() == 0)
        {
            return;
        }
        sortWindowValidate(streamWindows);
        for (Entry<String, List<PreviousExpression>> et : previous.entrySet())
        {
            List<PreviousExpression> expressions = et.getValue();
            IWindow win = streamWindows.get(et.getKey());
            if(win == null)
            {
                continue;
            }
            new PreviousServiceCreator().createAndSet(win, expressions);
        }
    }
    
    private void sortWindowValidate(Map<String, IWindow> streamWindows)
        throws ExecutorException
    {
        for (Entry<String, IWindow> et : streamWindows.entrySet())
        {
            IWindow win = et.getValue();
            if ((win instanceof TimeSortWindow) || (win instanceof LengthSortWindow))
            {
                ExecutorException exception =
                    new ExecutorException(ErrorCode.SEMANTICANALYZE_PREVIOUS_WITH_SORTWINDOW, win.getClass().getName());
                LOG.error("'PREVIOUS' can not used with Sort window.", exception);
                throw exception;
            }
        }
    }
  
    private Map<String, List<PreviousExpression>> getPrevious(IExpression[] select, IExpression where)
        throws ExecutorException
    {
        List<IExpression> previousExpressions = Lists.newArrayList();
        
        ExpressionsWalker getter = new ExpressionsWalker(new IsPreviousExpressionGetterStrategy());
        
        if (where != null)
        {
            getter.found(where, previousExpressions);
        }
        
        for (IExpression exp : select)
        {
            getter.found(exp, previousExpressions);
            
        }
        
        Map<String, List<PreviousExpression>> streamPrevious = new HashMap<String, List<PreviousExpression>>();
        for (IExpression ie : previousExpressions)
        {
            PreviousExpression pe = (PreviousExpression)ie;
            PropertyValueExpression pve = (PropertyValueExpression)pe.getProExpr();
            int streamIndex = pve.getStreamIndex();
            String streamName = inputSchemas.get(0).getStreamName();
            if (streamIndex == 1)
            {
                streamName = inputSchemas.get(1).getStreamName();
            }
            if (!streamPrevious.containsKey(streamName))
            {
                streamPrevious.put(streamName, new ArrayList<PreviousExpression>());
            }
            streamPrevious.get(streamName).add(pe);
        }
        return streamPrevious;
    }
    
    private SelectSubProcess createSelectProcessor()
        throws ExecutorException
    {
        String outputExression = basicAggOperator.getOutputExpression();
        
        IExpression[] exprs = new SelectViewExpressionCreator().create(inputSchemas, outputExression, systemConfig);
        
        if (exprs.length != outputSchemas.get(0).getCols().size())
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_NOTSAME_COLUMNS, String.valueOf(exprs.length),
                    String.valueOf(outputSchemas.get(0).getCols().size()));
            LOG.error("Select column don't match ouput stream column.", exception);
            throw exception;
        }
        
        SelectSubProcess jselet =
            new SelectSubProcess(transitionOut.getStreamName(), exprs, createHavingExpression(),
                streamschema.getEventType(transitionOut.getSchemaName()));
        return jselet;
    }
    
    private IExpression createHavingExpression()
        throws ExecutorException
    {
        String having = basicAggOperator.getFilterAfterAggregate();
        if (StringUtils.isEmpty(having))
        {
            return null;
        }
        
        IExpression filterExpression = new FilterViewExpressionCreator().create(outputSchemas, having, systemConfig);
        return filterExpression;
    }
    
}
