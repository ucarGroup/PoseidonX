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

package com.huawei.streaming.cql.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.UserFunction;
import com.huawei.streaming.api.opereators.AggregateOperator;
import com.huawei.streaming.api.opereators.BasicAggFunctionOperator;
import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.opereators.JoinFunctionOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.UnionOperator;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.builder.logicoptimizer.LogicOptimizer;
import com.huawei.streaming.cql.builder.operatorcombiner.OperatorCombiner;
import com.huawei.streaming.cql.builder.operatorsplitter.OperatorSplitter;
import com.huawei.streaming.cql.builder.operatorsplitter.SplitContext;
import com.huawei.streaming.cql.builder.operatorsplitter.SpliterTmps;
import com.huawei.streaming.cql.builder.physicoptimizer.PhysicOptimizer;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.executor.ExecutorUtils;
import com.huawei.streaming.cql.semanticanalyzer.BaseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.CreateStreamAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.InsertAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.InsertUserOperatorStatementAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertStatementAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;

/**
 * 应用程序构建器
 * 专门 用来完成从多个解析内容到应用程序的转换
 *
 */
public class ApplicationBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationBuilder.class);
    
    /**
     * 应用程序名称
     */
    private String applicationName;
    
    /**
     * 所有CQL语句的解析内容
     */
    private List<AnalyzeContext> parseContexts;
    
    /**
     * 构建完成的应用程序
     */
    private Application app;
    
    /**
     * 构建应用程序中用到的一些计数器保存位置
     */
    private BuilderUtils buildUtils;
    
    /**
     * 应用程序构建器
     * <默认构造函数>
     */
    public ApplicationBuilder()
    {
        buildUtils = DriverContext.getBuilderNameSpace().get();
    }
    
    /**
     * 构建应用程序
     * <p/>
     * 在构建应用程序之前，要先执行逻辑优化器
     * 在构建应用程序之后，要先执行物理优化器
     * <p/>
     * 整个应用程序的构建分如下几步：
     * 1、各个算子的构建
     * 2、将完成拆分的应用程序解析成为Application
     *
     */
    public Application build(String appName, List<AnalyzeContext> parContexts, DriverContext driverContext)
        throws ApplicationBuildException
    {
        this.applicationName = appName;
        this.parseContexts = parContexts;
        
        executeLogicOptimizer();
        buildApplication();
        executePhysicOptimizer();
        parseDriverContext(driverContext);
        
        return app;
    }
    
    private void parseDriverContext(DriverContext driverContext)
    {
        if (driverContext.getUserConfs() != null && driverContext.getUserConfs().size() != 0)
        {
            app.setConfs(driverContext.getUserConfs());
        }
        
        if (driverContext.getUserFiles() != null && driverContext.getUserFiles().size() != 0)
        {
            List<String> userFiles = driverContext.getUserFiles();
            app.setUserFiles(userFiles.toArray(new String[userFiles.size()]));
        }
        
        if (driverContext.getUserDefinedFunctions() != null && driverContext.getUserDefinedFunctions().size() != 0)
        {
            List<UserFunction> functions = Lists.newArrayList();
            for (UserFunction userFunction : driverContext.getUserDefinedFunctions().values())
            {
                functions.add(userFunction);
            }
            app.setUserFunctions(functions);
        }
    }
    
    /**
     * 执行逻辑优化器
     *
     */
    private void executeLogicOptimizer()
    {
        StreamingConfig conf = new StreamingConfig();
        LogicOptimizer logicOptimizer = new LogicOptimizer();
        logicOptimizer.init(conf);
        parseContexts = logicOptimizer.transform(parseContexts);
    }
    
    /**
     * 执行物理优化器
     *
     */
    private Application executePhysicOptimizer()
        throws ApplicationBuildException
    {
        PhysicOptimizer physicOptimizer = new PhysicOptimizer();
        this.app = physicOptimizer.optimize(app);
        return app;
    }
    
    /**
     * 构建应用程序
     *
     */
    private void buildApplication()
        throws ApplicationBuildException
    {
        app = new Application(applicationName);
        parseSchemas();
        List<SplitContext> splitContexts = splitOperators();
        SplitContext splitContext = combineOperators(splitContexts);
        
        changeUnionOperators(splitContext);
        changeSchemaAfterAggregate(splitContext);
        
        app.setOperators(splitContext.getOperators());
        app.setOpTransition(splitContext.getTransitions());
    }
    
    private SplitContext combineOperators(List<SplitContext> splitContexts)
        throws ApplicationBuildException
    {
        OperatorCombiner combiner = new OperatorCombiner(buildUtils);
        return combiner.combine(splitContexts);
    }
    
    /**
     * 替换所有的having和orderby这些在聚合之后的表达式
     *
     */
    private void changeSchemaAfterAggregate(SplitContext splitContext)
        throws ApplicationBuildException
    {
        for (Operator op : splitContext.getOperators())
        {
            if (!(op instanceof BasicAggFunctionOperator))
            {
                continue;
            }
            
            BasicAggFunctionOperator bop = (BasicAggFunctionOperator)op;
            replaceHaving(bop, splitContext);
            replaceOrderBy(bop, splitContext);
        }
    }
    
    private void replaceHaving(BasicAggFunctionOperator bop, SplitContext splitContext)
        throws ApplicationBuildException
    {
        if (StringUtils.isEmpty(bop.getFilterAfterAggregate()))
        {
            return;
        }
        
        List<OperatorTransition> toTransitions =
            ExecutorUtils.getTransitonsByFromId(bop.getId(), splitContext.getTransitions());
        
        Schema schema = BaseAnalyzer.getSchemaByName(toTransitions.get(0).getSchemaName(), app.getSchemas());
        String str = replaceColumns(bop, schema, bop.getFilterAfterAggregate());
        bop.setFilterAfterAggregate(str);
    }
    
    private String replaceColumns(BasicAggFunctionOperator bop, Schema schema, final String str)
    {
        String replacedStr = str;
        for (int i = 0; i < schema.getCols().size(); i++)
        {
            Column c = schema.getCols().get(i);
            String replaceedStr = SpliterTmps.formatIndex(i);
            PropertyValueExpressionDesc pv =
                new PropertyValueExpressionDesc(c, schema.getId(), getIndexInSchemas(bop, schema.getId()));
            replacedStr = replacedStr.replaceAll(replaceedStr, pv.toString());
        }
        return replacedStr;
    }
    
    private int getIndexInSchemas(BasicAggFunctionOperator bop, String schemaId)
    {
        if (bop instanceof AggregateOperator)
        {
            return 0;
        }
        
        JoinFunctionOperator jop = (JoinFunctionOperator)bop;
        if (jop.getLeftStreamName().equals(schemaId))
        {
            return 0;
        }
        return 1;
    }
    
    private void replaceOrderBy(BasicAggFunctionOperator bop, SplitContext splitContext)
        throws ApplicationBuildException
    {
        if (StringUtils.isEmpty(bop.getOrderBy()))
        {
            return;
        }
        List<OperatorTransition> totransitions =
            ExecutorUtils.getTransitonsByFromId(bop.getId(), splitContext.getTransitions());
        
        Schema schema = BaseAnalyzer.getSchemaByName(totransitions.get(0).getSchemaName(), app.getSchemas());
        String str = bop.getOrderBy();
        str = replaceColumns(bop, schema, str);
        bop.setOrderBy(str);
    }
    
    /**
     * union算子替换
     * <p/>
     * 1、找到所有的有一个以上连线的非Join算子
     * 2、替换该算子为Union算子
     *
     */
    private void changeUnionOperators(SplitContext splitContext)
    {
        Set<Operator> ops = getAllMultiConnectedOperators(splitContext);
        
        for (Operator op : ops)
        {
            /*
             * 目前仅支持对filter算子进行替换
             */
            if (op instanceof FilterOperator)
            {
                replaceToUnion((FilterOperator)op, splitContext);
            }
        }
    }
    
    /**
     * 替换union算子
     *
     */
    private void replaceToUnion(FilterOperator op, SplitContext splitContext)
    {
        UnionOperator uop = createUnionOperator(op);
        replaceToTransitions(op, splitContext, uop);
        replaceFromTransitions(op, splitContext, uop);
        splitContext.getOperators().remove(op);
        splitContext.getOperators().add(uop);
        resetUnionParallelNumber(splitContext, uop);
    }
    
    private void resetUnionParallelNumber(SplitContext splitContext, UnionOperator uop)
    {
        List<OperatorTransition> fromTransitions =
            ExecutorUtils.getTransitonsByFromId(uop.getId(), splitContext.getTransitions());
        for (OperatorTransition ot : fromTransitions)
        {
            Operator operatorAfterUnion =
                ExecutorUtils.getOperatorById(ot.getToOperatorId(), splitContext.getOperators());
            if (operatorAfterUnion != null)
            {
                uop.setParallelNumber(operatorAfterUnion.getParallelNumber());
            }
        }
    }
    
    private void replaceFromTransitions(FilterOperator op, SplitContext splitContext, UnionOperator uop)
    {
        List<OperatorTransition> fromTransitions =
            ExecutorUtils.getTransitonsByFromId(op.getId(), splitContext.getTransitions());
        
        for (OperatorTransition ot : fromTransitions)
        {
            ot.setFromOperatorId(uop.getId());
        }
    }
    
    private void replaceToTransitions(FilterOperator op, SplitContext splitContext, UnionOperator uop)
    {
        List<OperatorTransition> toTransitions =
            ExecutorUtils.getTransitonsByToId(op.getId(), splitContext.getTransitions());
        
        for (OperatorTransition ot : toTransitions)
        {
            ot.setToOperatorId(uop.getId());
        }
    }
    
    private UnionOperator createUnionOperator(FilterOperator fop)
    {
        UnionOperator uop = new UnionOperator(buildUtils.getNextOperatorName("Union"), 
            buildUtils.getDefaultParallelNumber());
        uop.setOutputExpression(fop.getOutputExpression());
        return uop;
    }
    
    private Set<Operator> getAllMultiConnectedOperators(SplitContext splitContext)
    {
        Map<String, List<String>> allOperatorTransitions =
            getAllOperatorIdByTransitionTo(splitContext.getTransitions());
        return getMultiOperators(allOperatorTransitions, splitContext);
    }
    
    private Set<Operator> getMultiOperators(Map<String, List<String>> allTransitions, SplitContext splitContext)
    {
        Set<Operator> ops = Sets.newHashSet();
        for (Entry<String, List<String>> et : allTransitions.entrySet())
        {
            if (et.getValue().size() > 1)
            {
                Operator op = ExecutorUtils.getOperatorById(et.getKey(), splitContext.getOperators());
                ops.add(op);
            }
        }
        return ops;
    }
    
    private Map<String, List<String>> getAllOperatorIdByTransitionTo(List<OperatorTransition> ts)
    {
        Map<String, List<String>> opts = Maps.newHashMap();
        for (int i = 0; i < ts.size(); i++)
        {
            String toopid = ts.get(i).getToOperatorId();
            String transitionId = ts.get(i).getId();
            
            if (!opts.containsKey(toopid))
            {
                opts.put(toopid, new ArrayList<String>());
            }
            opts.get(toopid).add(transitionId);
        }
        return opts;
    }
    
    private List<SplitContext> splitOperators()
        throws ApplicationBuildException
    {
        List<SplitContext> splitContexts = Lists.newArrayList();
        for (AnalyzeContext pContext : parseContexts)
        {
            parseAutoCreatePipeStream(splitContexts, pContext);
            parseSubQueryOperators(splitContexts, pContext);
            SplitContext context = OperatorSplitter.split(buildUtils, pContext);
            //create datasource 语句会产生空的context
            if (context == null)
            {
                continue;
            }
            splitContexts.add(context);
        }
        return splitContexts;
    }
    
    /**
     * 由于schema推断的存在， 中间的流schema没有通过create input语句定义，
     * 所以就要在这里显示的创建一个create input的解析内容
     *
     */
    private void parseAutoCreatePipeStream(List<SplitContext> splitContexts, AnalyzeContext pcontext)
        throws ApplicationBuildException
    {
        parseAutoCreatePipeStreamForInsert(splitContexts, pcontext);
        parseAutoCreatePipeStreamForMultiInsert(splitContexts, pcontext);
        parseAutoCreatePipeStreamForUserOperator(splitContexts, pcontext);
    }
    
    private void parseAutoCreatePipeStreamForUserOperator(List<SplitContext> splitContexts, AnalyzeContext pcontext)
        throws ApplicationBuildException
    {
        if (pcontext instanceof InsertUserOperatorStatementAnalyzeContext)
        {
            InsertUserOperatorStatementAnalyzeContext mpc = (InsertUserOperatorStatementAnalyzeContext)pcontext;
            if (!mpc.isPipeStreamNotCreated())
            {
                return;
            }
            SplitContext sc = createPipeStreamSplitContext(mpc.getOutputSchema());
            splitContexts.add(sc);
        }
    }
    
    private void parseAutoCreatePipeStreamForMultiInsert(List<SplitContext> splitContexts, AnalyzeContext pcontext)
        throws ApplicationBuildException
    {
        if (pcontext instanceof MultiInsertStatementAnalyzeContext)
        {
            MultiInsertStatementAnalyzeContext mpc = (MultiInsertStatementAnalyzeContext)pcontext;
            for (MultiInsertAnalyzeContext mInsert : mpc.getMultiSelectBodyAnalyzeContexts())
            {
                if (!mInsert.isPipeStreamNotCreated())
                {
                    continue;
                }
                SplitContext sc = createPipeStreamSplitContext(mInsert.getOutputSchema());
                splitContexts.add(sc);
            }
        }
    }

    private void parseAutoCreatePipeStreamForInsert(List<SplitContext> splitContexts, AnalyzeContext pcontext)
        throws ApplicationBuildException
    {
        if (pcontext instanceof InsertAnalyzeContext)
        {
            InsertAnalyzeContext ipc = (InsertAnalyzeContext)pcontext;
            if (!ipc.isPipeStreamNotCreated())
            {
                return;
            }
            SplitContext sc = createPipeStreamSplitContext(ipc.getOutputSchema());
            splitContexts.add(sc);
        }
    }
    
    private void parseSubQueryOperators(List<SplitContext> splitContexts, AnalyzeContext pcontext)
        throws ApplicationBuildException
    {
        if (pcontext instanceof InsertAnalyzeContext)
        {
            InsertAnalyzeContext ipc = (InsertAnalyzeContext)pcontext;
            parseStreamContextInSubQuery(splitContexts, ipc);
            parseInsertContextInSubQuery(splitContexts, ipc);
        }
        
        if (pcontext instanceof MultiInsertStatementAnalyzeContext)
        {
            MultiInsertStatementAnalyzeContext mpc = (MultiInsertStatementAnalyzeContext)pcontext;
            parseStreamContextInSubQuery(splitContexts, mpc);
            parseInsertContextInSubQuery(splitContexts, mpc);
        }
    }
    
    private void parseInsertContextInSubQuery(List<SplitContext> splitContexts, InsertAnalyzeContext ipc)
        throws ApplicationBuildException
    {
        Map<String, InsertAnalyzeContext> subs = ipc.getSelectContext().getFromClauseContext().getSubQueryForStream();
        for (Entry<String, InsertAnalyzeContext> et : subs.entrySet())
        {
            InsertAnalyzeContext insertContext = et.getValue();
            insertContext.getSelectContext()
                .setParallelClauseContext(ipc.getSelectContext().getParallelClauseContext());
            parseSubQueryOperators(splitContexts, et.getValue());
            SplitContext context = OperatorSplitter.split(buildUtils, insertContext);
            splitContexts.add(context);
        }
    }
    
    private void parseInsertContextInSubQuery(List<SplitContext> splitContexts, MultiInsertStatementAnalyzeContext ipc)
        throws ApplicationBuildException
    {
        Map<String, InsertAnalyzeContext> subs = ipc.getFrom().getSubQueryForStream();
        for (Entry<String, InsertAnalyzeContext> et : subs.entrySet())
        {
            InsertAnalyzeContext insertContext = et.getValue();
            insertContext.getSelectContext().setParallelClauseContext(ipc.getParallelClause());
            parseSubQueryOperators(splitContexts, et.getValue());
            SplitContext context = OperatorSplitter.split(buildUtils, insertContext);
            splitContexts.add(context);
        }
    }
    
    private void parseStreamContextInSubQuery(List<SplitContext> splitContexts, InsertAnalyzeContext ipc)
        throws ApplicationBuildException
    {
        Map<String, CreateStreamAnalyzeContext> subCreateStreamContexts =
            ipc.getSelectContext().getFromClauseContext().getSubQuerySchemas();
        for (Entry<String, CreateStreamAnalyzeContext> et : subCreateStreamContexts.entrySet())
        {
            SplitContext context = OperatorSplitter.split(buildUtils, et.getValue());
            splitContexts.add(context);
        }
    }
    
    private void parseStreamContextInSubQuery(List<SplitContext> splitContexts, MultiInsertStatementAnalyzeContext ipc)
        throws ApplicationBuildException
    {
        Map<String, CreateStreamAnalyzeContext> subCreateStreamContexts = ipc.getFrom().getSubQuerySchemas();
        for (Entry<String, CreateStreamAnalyzeContext> et : subCreateStreamContexts.entrySet())
        {
            SplitContext context = OperatorSplitter.split(buildUtils, et.getValue());
            splitContexts.add(context);
        }
    }
    
    private void parseSchemas()
    {
        List<Schema> schemas = Lists.newArrayList();
        for (AnalyzeContext context : parseContexts)
        {
            schemas.addAll(context.getCreatedSchemas());
        }
        app.setSchemas(schemas);
    }
    
    private SplitContext createPipeStreamSplitContext(Schema schema)
        throws ApplicationBuildException
    {
        LOG.info("create pipe Stream while stream is not created!");
        CreateStreamAnalyzeContext pipe = new CreateStreamAnalyzeContext();
        pipe.setSchema(schema);
        pipe.setStreamName(schema.getId());
        return OperatorSplitter.split(buildUtils, pipe);
    }
}
