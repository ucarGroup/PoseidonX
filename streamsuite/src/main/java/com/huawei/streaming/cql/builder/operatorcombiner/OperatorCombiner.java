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

package com.huawei.streaming.cql.builder.operatorcombiner;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.SplitterOperator;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.builder.operatorsplitter.OperatorSplitter;
import com.huawei.streaming.cql.builder.operatorsplitter.SplitContext;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.BaseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.CreateStreamAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.InsertAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.InsertUserOperatorStatementAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertStatementAnalyzeContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 将多个算子组合起来
 * 组件算子之间的上下级关系
 * <p/>
 * 算子之间的连线，有两种来源：
 * 1、算子是由一条CQL语句拆分出多个算子组成，
 * 这样，连线就可以在拆分的时候确定。
 * 2、算是是由多条CQL语句组合而来，
 * 通过使用insert into
 * select from 这样的语句，
 * 就可以实现多个算子之间的级联。
 * 甚至可以改变算子之间的连接关系。
 * 比如在aggregate算子之前加入union算子
 * 在aggregate算子之后加入split算子。
 * <p/>
 * 数据分发类型都应该是Shuffle的方式
 *
 */
public class OperatorCombiner
{
    private static final Logger LOG = LoggerFactory.getLogger(OperatorSplitter.class);
    
    private BuilderUtils buildUtils;
    
    private Map<String, SplitContext> inputStreams = Maps.newHashMap();
    
    private Map<String, SplitContext> outputStreams = Maps.newHashMap();
    
    private Map<String, SplitContext> pipeStreams = Maps.newHashMap();
    
    private SplitContext result = new SplitContext();
    
    /**
     * <默认构造函数>
     *
     */
    public OperatorCombiner(BuilderUtils buildUtils)
    {
        this.buildUtils = buildUtils;
    }
    
    /**
     * 多个算子之间的合并。
     *
     */
    public SplitContext combine(List<SplitContext> splitContexts)
        throws ApplicationBuildException
    {
        LOG.info("combine all split contexts");
        if (splitContexts == null)
        {
            return result;
        }
        
        for (int i = 0; i < splitContexts.size(); i++)
        {
            combineSplitContext(splitContexts.get(i));
        }
        return result;
    }
    
    private void combineSplitContext(SplitContext context)
        throws ApplicationBuildException
    {
        result.getOperators().addAll(context.getOperators());
        result.getTransitions().addAll(context.getTransitions());
        
        /*
         * 目前只有两种CQL语句
         * 一种是流定义语句
         * 一种就是Insert into语句
         */
        if (context.getParseContext() instanceof CreateStreamAnalyzeContext)
        {
            addSchemasFromCreateStream(context);
            return;
        }
        
        createTransition(context);
    }
    
    /**
     * 为每个insert into select语句解析出来的结果加入上下文连线。
     * <p/>
     * CQL语句之间的连线，必然从inputStream或者PipeStream发起，
     * 连接到outputStream或者PipeStream.
     * <p/>
     * 首先，找到insert into语句中计算出来的连线的起点。找到对应的算子
     * 然后，根据起点的schema名称，找到对应的流名称，创建连线
     * <p/>
     * 连线上的Schema名称，就是流名称！
     *
     */
    private void createTransition(SplitContext context)
        throws ApplicationBuildException
    {
        if (context.getParseContext() instanceof MultiInsertStatementAnalyzeContext)
        {
            MultiInsertStatementAnalyzeContext multiInsertContext =
                (MultiInsertStatementAnalyzeContext)context.getParseContext();
            createFromTransition(context, multiInsertContext);
            createToTransition(context, multiInsertContext);
        }
        else if (context.getParseContext() instanceof InsertUserOperatorStatementAnalyzeContext)
        {
            InsertUserOperatorStatementAnalyzeContext insertUserOperatorContext =
                (InsertUserOperatorStatementAnalyzeContext)context.getParseContext();
            createFromTransition(context, insertUserOperatorContext);
            createToTransition(context, insertUserOperatorContext);
        }
        else
        {
            InsertAnalyzeContext insertContext = (InsertAnalyzeContext)context.getParseContext();
            createFromTransition(context, insertContext);
            createToTransition(context, insertContext);
        }
    }
    
    private void createToTransition(SplitContext context, InsertAnalyzeContext insertContext)
        throws ApplicationBuildException
    {
        Set<Operator> ops = getLastOperator(context);
        for (Operator op : ops)
        {
            String startStreamName = insertContext.getOutputStreamName();
            SplitContext toContext = getToSplitContext(startStreamName);
            Schema schema = insertContext.getOutputSchema();
            String nextStreamName = buildUtils.getNextStreamName();
            
            Operator toOp = toContext.getFirstOperator();
            if (toOp == null)
            {
                SemanticAnalyzerException exception = 
                    new SemanticAnalyzerException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
                LOG.error("Can't find receive operator.", exception);
                throw exception;
            }
            OperatorTransition totransition =
                new OperatorTransition(nextStreamName, op, toOp, DistributeType.SHUFFLE, null, schema);
            result.addTransitions(totransition);
        }
    }
    
    private void createToTransition(SplitContext context, InsertUserOperatorStatementAnalyzeContext insertContext)
        throws ApplicationBuildException
    {
        Set<Operator> ops = getLastOperator(context);
        for (Operator op : ops)
        {
            String startStreamName = insertContext.getOutputStreamName();
            SplitContext toContext = getToSplitContext(startStreamName);
            Schema schema = insertContext.getOutputSchema();
            String nextStreamName = buildUtils.getNextStreamName();
            
            Operator toOp = toContext.getFirstOperator();
            if (toOp == null)
            {
                SemanticAnalyzerException exception = 
                    new SemanticAnalyzerException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
                LOG.error("Can't find receive operator.", exception);
                throw exception;
            }
            OperatorTransition totransition =
                new OperatorTransition(nextStreamName, op, toOp, DistributeType.SHUFFLE, null, schema);
            result.addTransitions(totransition);
        }
    }
    
    private void createToTransition(SplitContext context, MultiInsertStatementAnalyzeContext insertContext)
        throws ApplicationBuildException
    {
        Set<Operator> ops = getLastOperator(context);
        for (Operator op : ops)
        {
            
            SplitterOperator sop = (SplitterOperator)op;
            
            for (int i = 0; i < insertContext.getMultiSelectBodyAnalyzeContexts().size(); i++)
            {
                MultiInsertAnalyzeContext multiInsert = insertContext.getMultiSelectBodyAnalyzeContexts().get(i);
                String startStreamName = multiInsert.getOutputStreamName();
                SplitContext toContext = getToSplitContext(startStreamName);
                Schema schema = multiInsert.getOutputSchema();
                String nextStreamName = buildUtils.getNextStreamName();
                sop.getSubSplitters().get(i).setStreamName(nextStreamName);
                Operator toOp = toContext.getFirstOperator();
                if (toOp == null)
                {
                    SemanticAnalyzerException exception = 
                        new SemanticAnalyzerException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
                    LOG.error("Can't find receive operator.", exception);
                    throw exception;
                }
                OperatorTransition toTransition =
                    new OperatorTransition(nextStreamName, sop, toOp, DistributeType.SHUFFLE, null, schema);
                result.addTransitions(toTransition);
            }
        }
    }
    
    private Set<Operator> getLastOperator(SplitContext context)
    {
        Set<Operator> ops = Sets.newHashSet();
        List<OperatorTransition> endTransitions = context.getLastTransitons();
        for (OperatorTransition transition : endTransitions)
        {
            Operator op = context.getOperatorById(transition.getToOperatorId());
            ops.add(op);
        }
        return ops;
    }
    
    private void createFromTransition(SplitContext context, MultiInsertStatementAnalyzeContext insertContext)
        throws SemanticAnalyzerException
    {
        List<OperatorTransition> startTransitions = context.getFirstTransitons();
        for (OperatorTransition transition : startTransitions)
        {
            Operator op = context.getOperatorById(transition.getFromOperatorId());
            if (op == null)
            {
                SemanticAnalyzerException exception = 
                    new SemanticAnalyzerException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
                LOG.error("Can't find opertor by transition from id : {} .", transition.getFromOperatorId(), exception);
                throw exception;
            }
            String startStreamName = transition.getSchemaName();
            SplitContext fromContext = getFromSplitContext(startStreamName);
            Schema schema = getInputSchema(startStreamName, insertContext);
            String nextStreamName = buildUtils.getNextStreamName();
            
            Operator fromOp = fromContext.getLastOperator();  
            
            OperatorTransition fromtransition =
                new OperatorTransition(nextStreamName, fromOp, op, DistributeType.SHUFFLE, null, schema);
            result.addTransitions(fromtransition);
            
        }
    }
    
    private void createFromTransition(SplitContext context, InsertUserOperatorStatementAnalyzeContext insertContext)
        throws SemanticAnalyzerException
    {
        List<OperatorTransition> startTransitions = context.getFirstTransitons();
        for (OperatorTransition transition : startTransitions)
        {
            Operator op = context.getOperatorById(transition.getFromOperatorId());
            if (op == null)
            {
                SemanticAnalyzerException exception = 
                    new SemanticAnalyzerException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
                LOG.error("Can't find opertor by transition from id : {} .", transition.getFromOperatorId(), exception);
                throw exception;
            }
            String startStreamName = transition.getSchemaName();
            SplitContext fromContext = getFromSplitContext(startStreamName);
            Schema schema = getInputSchema(startStreamName, insertContext);
            String nextStreamName = buildUtils.getNextStreamName();
            
            Operator fromOp = fromContext.getLastOperator();  
            
            OperatorTransition fromtransition =
                new OperatorTransition(nextStreamName, fromOp, op, DistributeType.SHUFFLE, null, schema);
            result.addTransitions(fromtransition);
            
        }
    }
    
    private void createFromTransition(SplitContext context, InsertAnalyzeContext insertContext)
        throws SemanticAnalyzerException
    {
        List<OperatorTransition> startTransitions = context.getFirstTransitons();
        for (OperatorTransition transition : startTransitions)
        {
            Operator op = context.getOperatorById(transition.getFromOperatorId());
            if (op == null)
            {
                SemanticAnalyzerException exception = 
                    new SemanticAnalyzerException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
                LOG.error("Can't find opertor by transition from id : {} .", transition.getFromOperatorId(), exception);
                throw exception;
            }
            String startStreamName = transition.getSchemaName();
            SplitContext fromContext = getFromSplitContext(startStreamName);
            Schema schema = getInputSchema(startStreamName, insertContext);
            String nextStreamName = buildUtils.getNextStreamName();
            
            Operator fromOp = fromContext.getLastOperator();
            
            OperatorTransition fromtransition =
                new OperatorTransition(nextStreamName, fromOp, op, DistributeType.SHUFFLE, null, schema);
            result.addTransitions(fromtransition);
            
        }
    }
    
    /**
     * 获取输入shcema信息
     *
     */
    private Schema getInputSchema(String streamName, InsertAnalyzeContext insertContext)
        throws SemanticAnalyzerException
    {
        List<Schema> schemas = insertContext.getSelectContext().getFromClauseContext().getInputSchemas();
        return BaseAnalyzer.getSchemaByName(streamName, schemas);
    }
    
    
    private Schema getInputSchema(String streamName, InsertUserOperatorStatementAnalyzeContext insertContext)
        throws SemanticAnalyzerException
    {
        List<Schema> schemas = Lists.newArrayList(insertContext.getInputSchmea());
        return BaseAnalyzer.getSchemaByName(streamName, schemas);
    }
    
    
    private Schema getInputSchema(String streamName, MultiInsertStatementAnalyzeContext insertContext)
        throws SemanticAnalyzerException
    {
        List<Schema> schemas = insertContext.getFrom().getInputSchemas();
        return BaseAnalyzer.getSchemaByName(streamName, schemas);
    }
    
    private SplitContext getFromSplitContext(String streamName)
    {
        if (inputStreams.containsKey(streamName))
        {
            return inputStreams.get(streamName);
        }
        return pipeStreams.get(streamName);
    }
    
    private SplitContext getToSplitContext(String streamName)
    {
        if (outputStreams.containsKey(streamName))
        {
            return outputStreams.get(streamName);
        }
        return pipeStreams.get(streamName);
    }
    
    private void addSchemasFromCreateStream(SplitContext context)
    {
        CreateStreamAnalyzeContext inputContext = (CreateStreamAnalyzeContext)context.getParseContext();
        String streamName = inputContext.getStreamName();
        
        if (inputContext.getDeserializerClassName() != null && inputContext.getSerializerClassName() == null)
        {
            inputStreams.put(streamName, context);
        }
        
        if (inputContext.getDeserializerClassName() == null && inputContext.getSerializerClassName() != null)
        {
            outputStreams.put(streamName, context);
        }
        
        if (inputContext.getDeserializerClassName() == null && inputContext.getSerializerClassName() == null)
        {
            pipeStreams.put(streamName, context);
        }
        
    }
}
