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

package com.huawei.streaming.cql.executor.operatorinfocreater;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.util.StreamingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.CombineOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.expressioncreater.PropertyValueExpressionCreator;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzerFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.PropertyValueExpression;
import com.huawei.streaming.operator.AbsOperator;
import com.huawei.streaming.operator.functionstream.CombineFunctionOp;

/**
 * 创建combine算子实例
 *
 */
public class CombineInfoCreator implements OperatorInfoCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(CombineInfoCreator.class);
    
    private CombineOperator combineOperator;
    
    private List<OperatorTransition> transitionIn = null;
    
    private OperatorTransition transitionOut = null;
    
    private List<Schema> inputSchemas = Lists.newArrayList();
    
    private IEventType outputTupleEvent = null;
    
    private Map<String, String> applicationConfig;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AbsOperator createInstance(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemConfig)
     throws StreamingException
    {
        prepare(vapp, operator, streamschema, systemConfig);
        
        for (OperatorTransition s : transitionIn)
        {
            inputSchemas.addAll(OperatorInfoCreatorFactory.getSchemasByTransition(vapp, s));
        }
        
        CombineFunctionOp combine = new CombineFunctionOp();
        StreamingConfig config = new StreamingConfig();
        if (operator.getArgs() != null)
        {
            config.putAll(operator.getArgs());
        }
        config.putAll(this.applicationConfig);
        
        setInputStreams(config);
        setCombineConditions(config);
        setInputSchemas(streamschema, config);
        setCombineOutputs(config);
        config.put(StreamingConfig.STREAMING_INNER_OUTPUT_SCHEMA, StreamingUtils.serializeSchema((TupleEventType) outputTupleEvent));
        config.put(StreamingConfig.STREAMING_INNER_OUTPUT_STREAM_NAME, transitionOut.getStreamName());
        combine.setConfig(config);
        
        return OperatorInfoCreatorFactory.buildStreamOperator(operator, combine);
    }
    
    private void prepare(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemconfig)
        throws ExecutorException
    {
        this.applicationConfig = systemconfig;
        this.combineOperator = (CombineOperator)operator;
        this.transitionIn = OperatorInfoCreatorFactory.getTransitionIn(vapp, operator);
        this.transitionOut = OperatorInfoCreatorFactory.getTransitionOut(vapp, operator);
        this.outputTupleEvent = streamschema.getEventType(transitionOut.getSchemaName());
    }
    
    private void setCombineOutputs(StreamingConfig config)
        throws ExecutorException
    {
        
        List<ExpressionDescribe> exps = createCombineExpressions(combineOperator.getOutputExpression());
        
        /*
         * 需要检查相同的流属性必须放在一起
         */
        List<String> combineStreams = Lists.newArrayList();
        List<List<PropertyValueExpression>> tmpOuts = Lists.newArrayList();
        
        for (int i = 0; i < exps.size(); i++)
        {
            if (!(exps.get(i) instanceof PropertyValueExpressionDesc))
            {
                
                ExecutorException exception =
                    new ExecutorException(ErrorCode.SEMANTICANALYZE_COMBINE_SIMPLE_EXPRESSION);
                LOG.error("Not property value expression in combie clause.", exception);
                
                throw exception;
            }
            PropertyValueExpressionDesc pexp = (PropertyValueExpressionDesc)exps.get(i);
            String streamName = pexp.getSchemaId();
            PropertyValueExpression expression =
                (PropertyValueExpression)new PropertyValueExpressionCreator().createInstance(pexp, null);
            
            if (combineStreams.size() == 0)
            {
                combineStreams.add(streamName);
                tmpOuts.add(new ArrayList<PropertyValueExpression>());
                tmpOuts.get(tmpOuts.size() - 1).add(expression);
            }
            else
            {
                if (combineStreams.get(combineStreams.size() - 1).equals(streamName))
                {
                    tmpOuts.get(tmpOuts.size() - 1).add(expression);
                }
                else
                {
                    /*
                     * 如果出现不一样的流名称，先检查该流名称是否已经出现过
                     * 如果出现过，就报错，因为combine中的列必须是按照顺序进行的。   
                     */
                    checkStreamNameRepeated(combineStreams, streamName);
                    
                    combineStreams.add(streamName);
                    tmpOuts.add(new ArrayList<PropertyValueExpression>());
                    tmpOuts.get(tmpOuts.size() - 1).add(expression);
                }
            }
            
        }
        //查询顺序取决于这个参数
        config.put(StreamingConfig.OPERATOR_COMBINE_INPUTNAMES_AND_EXPRESSION,
            createOutputMaps(combineStreams, tmpOuts));
    }
    
    private Map<String, IExpression[]> createOutputMaps(List<String> combineStreams,
        List<List<PropertyValueExpression>> tmpOuts)
        throws ExecutorException
    {
        Map<String, IExpression[]> outSelect = Maps.newHashMap();
        for (int i = 0; i < combineStreams.size(); i++)
        {
            String streamName = combineStreams.get(i);
            List<PropertyValueExpression> exps = tmpOuts.get(i);
            outSelect.put(getPlanStreamNameFromTransition(streamName),
                exps.toArray(new PropertyValueExpression[exps.size()]));
        }
        return outSelect;
    }
    
    private void checkStreamNameRepeated(List<String> combineStreams, String streamName)
        throws ExecutorException
    {
        for (String str : combineStreams)
        {
            if (streamName.equals(str))
            {
                ExecutorException exception =
                    new ExecutorException(ErrorCode.SEMANTICANALYZE_COMBINE_EXPRESSION_TOGETHER);
                LOG.error("Columns in same stream not together in combine select clause.", exception);
                throw exception;
            }
        }
    }
    
    private void setInputSchemas(EventTypeMng streamschema, StreamingConfig config)
    {
        Map<String, IEventType> inputschemas = Maps.newHashMap();
        
        for (OperatorTransition s : transitionIn)
        {
            IEventType iet = streamschema.getEventType(s.getSchemaName());
            inputschemas.put(s.getStreamName(), iet);
        }
        config.put(StreamingConfig.OPERATOR_COMBINE_INPUTNAMES_AND_SCHEMA, inputschemas);
    }
    
    private void setInputStreams(StreamingConfig config)
        throws ExecutorException
    {
        String orderedStreams = combineOperator.getOrderedStreams();
        List<String> inputStreamNames = Lists.newArrayList();
        String[] strs = orderedStreams.split(",");
        for (int i = 0; i < strs.length; i++)
        {
            inputStreamNames.add(getPlanStreamNameFromTransition(strs[i]));
        }
        
        config.put(StreamingConfig.OPERATOR_COMBINE_INPUTNAMES, inputStreamNames);
    }
    
    private void setCombineConditions(StreamingConfig config)
        throws ExecutorException
    {
        Map<String, String> keyMap = Maps.newHashMap();
        List<ExpressionDescribe> exps = createCombineExpressions(combineOperator.getCombineProperties());
        
        for (ExpressionDescribe exp : exps)
        {
            PropertyValueExpressionDesc pexp = (PropertyValueExpressionDesc)exp;
            String streamName = pexp.getSchemaId();
            keyMap.put(getPlanStreamNameFromTransition(streamName), pexp.getProperty());
        }
        config.put(StreamingConfig.OPERATOR_COMBINE_INPUTNAMES_AND_KEY, keyMap);
    }
    
    private String getPlanStreamNameFromTransition(String opStreamName)
        throws ExecutorException
    {
        for (OperatorTransition ot : transitionIn)
        {
            if (ot.getSchemaName().equals(opStreamName))
            {
                return ot.getStreamName();
            }
        }
        
        ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_NOFOUND_STREAM, opStreamName);
        LOG.error("Can't find stream from transition by stream name {}.", opStreamName, exception);
        throw exception;
    }
    
    private List<ExpressionDescribe> createCombineExpressions(String selectClause)
        throws SemanticAnalyzerException
    {
        IParser parser = ParserFactory.createSelectClauseParser();
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parser.parse(selectClause), inputSchemas);
        SelectClauseAnalyzeContext analyzeContext = (SelectClauseAnalyzeContext)analyzer.analyze();
        return analyzeContext.getExpdes();
    }
}
