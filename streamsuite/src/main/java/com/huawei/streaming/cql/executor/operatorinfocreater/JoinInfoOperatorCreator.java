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

import java.util.List;
import java.util.Map;

import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.util.StreamingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.JoinFunctionOperator;
import com.huawei.streaming.api.opereators.JoinType;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.UniDiRectionType;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.operatorviewscreater.AggResultSetMergeViewCreator;
import com.huawei.streaming.cql.executor.operatorviewscreater.AggResultSetParameters;
import com.huawei.streaming.cql.executor.operatorviewscreater.FilterViewExpressionCreator;
import com.huawei.streaming.cql.executor.operatorviewscreater.WindowViewCreator;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.ExpressionOperator;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.LogicExpression;
import com.huawei.streaming.expression.PropertyValueExpression;
import com.huawei.streaming.expression.RelationExpression;
import com.huawei.streaming.operator.AbsOperator;
import com.huawei.streaming.operator.FunctionOperator;
import com.huawei.streaming.operator.functionstream.JoinFunctionOp;
import com.huawei.streaming.operator.functionstream.SelfJoinFunctionOp;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.agg.resultmerge.IAggResultSetMerge;
import com.huawei.streaming.process.join.CrossBiJoinComposer;
import com.huawei.streaming.process.join.FullOutBiJoinComposer;
import com.huawei.streaming.process.join.IJoinComposer;
import com.huawei.streaming.process.join.IndexedMultiPropertyEventCollection;
import com.huawei.streaming.process.join.InnerBiJoinComposer;
import com.huawei.streaming.process.join.JoinFilterProcessor;
import com.huawei.streaming.process.join.SideBiJoinComposer;
import com.huawei.streaming.process.join.SideJoinType;
import com.huawei.streaming.process.join.SimpleEventCollection;
import com.huawei.streaming.window.IWindow;

/**
 * 创建join算子实例
 * IndexedMultiPropertyEventCollection的参数是一一对应的，比如 S1.id=S2.id,S1.name=s2.cname
 * 那么，left表达式中，表达式数组应该是 s1.id,s1.name
 * right表达式中，表达式数组应该是s2.id,s2.cname
 * <p/>
 * 表达式比如是propertyvalue表达式，目前不支持常量表达式或者 boolean表达式
 * 其他不等值join，必须放在crossjoin中实现
 *
 */
public class JoinInfoOperatorCreator implements OperatorInfoCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(JoinInfoOperatorCreator.class);
    
    private static final int JOINEXPRESSIONARRLENGHT = 2;
    
    private static final String DEFAULT_LEFT_SELFJOIN_STREAM_NAME = "left_self";
    
    private static final String DEFAULT_RIGHT_SELFJOIN_STREAM_NAME = "right_self";
    
    private JoinFunctionOperator joinOperator;
    
    private OperatorTransition leftTransitionIn = null;
    
    private OperatorTransition rightTransitionIn = null;
    
    private OperatorTransition transitionOut = null;
    
    private List<Schema> leftInputSchemas;
    
    private List<Schema> rightInputSchemas;
    
    private List<Schema> outputSchemas;
    
    private IEventType leftInputTupleEvent = null;
    
    private IEventType rightInputTupleEvent = null;
    
    private Map<String, String> applicationConfig;
    
    private OutputType outputType;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AbsOperator createInstance(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemConfig)
        throws StreamingException
    {
        prepare(vapp, operator, streamschema, systemConfig);
        outputType =
            this.getOutputType(joinOperator.getLeftWindow(),
                joinOperator.getRightWindow(),
                joinOperator.getUniDirectionIndex());
        IWindow leftWindow =
            new WindowViewCreator().create(leftInputSchemas, joinOperator.getLeftWindow(), systemConfig);
        IWindow rightWindow =
            new WindowViewCreator().create(rightInputSchemas, joinOperator.getRightWindow(), systemConfig);
        
        JoinFilterProcessor jfProcessor = createJoinFilterProcessor();
        
        IExpression filterExp = null;
        if (jfProcessor != null)
        {
            filterExp = jfProcessor.getExpr();
        }
        
        AggResultSetParameters pars = createResultSetMergeParmeters(streamschema, leftWindow, rightWindow, filterExp);
        IAggResultSetMerge joinProcessor = new AggResultSetMergeViewCreator(pars).create();
        
        FunctionOperator joinop = createJoinOperator(leftWindow, rightWindow, jfProcessor, joinProcessor);
        
        StreamingConfig config = new StreamingConfig();
        config.putAll(this.applicationConfig);
        setUniDirectionConfig(config);
        setJoinConfig(config);
        joinop.setConfig(config);
        return OperatorInfoCreatorFactory.buildStreamOperator(operator, joinop);
    }
    
    private FunctionOperator createJoinOperator(IWindow leftWindow, IWindow rightWindow,
        JoinFilterProcessor jfProcessor, IAggResultSetMerge joinProcessor)
        throws ExecutorException
    {
        if (isSelfJoin())
        {
            //selfJoin中只支持单流输出
            if (UniDiRectionType.NONE_STREAM == joinOperator.getUniDirectionIndex())
            {
                ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_UNIDIRECTION_SELFJOIN);
                LOG.error("No Unidirection in selfJoin.", exception);
                throw exception;
            }

            return new SelfJoinFunctionOp(leftWindow, rightWindow, createJoinComposer(), jfProcessor, joinProcessor,
                outputType);
        }
        return new JoinFunctionOp(leftWindow, rightWindow, createJoinComposer(), jfProcessor, joinProcessor, outputType);
    }
    
    private void prepare(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemconfig)
        throws ExecutorException
    {
        this.applicationConfig = systemconfig;
        this.joinOperator = (JoinFunctionOperator)operator;
        this.leftTransitionIn =
            OperatorInfoCreatorFactory.getTransitionIn(vapp, operator, joinOperator.getLeftStreamName());
        this.rightTransitionIn =
            OperatorInfoCreatorFactory.getTransitionIn(vapp, operator, joinOperator.getRightStreamName());
        
        this.transitionOut = OperatorInfoCreatorFactory.getTransitionOut(vapp, operator);
        this.leftInputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, leftTransitionIn);
        this.rightInputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, rightTransitionIn);
        this.outputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, transitionOut);
        this.leftInputTupleEvent = streamschema.getEventType(leftTransitionIn.getSchemaName());
        this.rightInputTupleEvent = streamschema.getEventType(rightTransitionIn.getSchemaName());
    }
    
    private AggResultSetParameters createResultSetMergeParmeters(EventTypeMng streamschema, IWindow leftWindow,
        IWindow rightWindow, IExpression whereExpression)
        throws ExecutorException
    {
        /*
         * 这里的list中的schema的顺序不能乱，第一个一定是left，第二个一定是right
         * 因为后面select表达式中需要使用
         */
        List<Schema> inputSchemas = Lists.newArrayList();
        inputSchemas.addAll(cloneSchema(leftInputSchemas, leftTransitionIn.getStreamName()));
        inputSchemas.addAll(cloneSchema(rightInputSchemas, rightTransitionIn.getStreamName()));
        
        Map<String, IWindow> streamWindows = Maps.newHashMap();
        streamWindows.put(leftInputSchemas.get(0).getStreamName(), leftWindow);
        streamWindows.put(rightInputSchemas.get(0).getStreamName(), rightWindow);
        
        List<Window> operatorWindows = Lists.newArrayList();
        operatorWindows.add(joinOperator.getLeftWindow());
        operatorWindows.add(joinOperator.getRightWindow());
        
        AggResultSetParameters pars = new AggResultSetParameters();
        pars.setBasicAggOperator(joinOperator);
        pars.setInputSchemas(inputSchemas);
        pars.setStreamschema(streamschema);
        pars.setOutputSchemas(outputSchemas);
        pars.setTransitionOut(transitionOut);
        pars.setStreamWindows(streamWindows);
        pars.setExpressionBeforeAggregate(whereExpression);
        pars.setSystemConfig(applicationConfig);
        pars.setOperatorWindows(operatorWindows);
        return pars;
    }
    
    private void setJoinConfig(StreamingConfig config)
    {
        config.put(StreamingConfig.OPERATOR_JOIN_INNER_LEFT_INPUT_STREAM_NAME, joinOperator.getLeftStreamName());
        config.put(StreamingConfig.OPERATOR_JOIN_INNER_RIGHT_INPUT_STREAM_NAME, joinOperator.getRightStreamName());
        config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_LEFT_INPUT_STREAM_NAME, DEFAULT_LEFT_SELFJOIN_STREAM_NAME);
        config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_RIGHT_INPUT_STREAM_NAME, DEFAULT_RIGHT_SELFJOIN_STREAM_NAME);
        config.put(StreamingConfig.OPERATOR_JOIN_INNER_LEFT_SCHEMA, StreamingUtils.serializeSchema((TupleEventType) leftInputTupleEvent));
        config.put(StreamingConfig.OPERATOR_JOIN_INNER_RIGHT_SCHEMA, StreamingUtils.serializeSchema((TupleEventType) rightInputTupleEvent));
        config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_INPUT_SCHEMA, StreamingUtils.serializeSchema((TupleEventType) leftInputTupleEvent));
    }
    
    private void setUniDirectionConfig(StreamingConfig config)
    {
        if (joinOperator.getUniDirectionIndex() == null)
        {
            config.put(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL, false);
            
            config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL, false);
            return;
        }
        switch (joinOperator.getUniDirectionIndex())
        {
            case LEFT_STREAM:
                config.put(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL, true);
                config.put(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL_INDEX, 0);
                config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL, true);
                config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL_INDEX, 0);
                break;
            case RIGHT_STREAM:
                config.put(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL, true);
                config.put(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL_INDEX, 1);
                config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL, true);
                config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL_INDEX, 1);
                break;
            default:
                config.put(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL, false);
                config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL, false);
                break;
        }
        
    }
    
    /**
     * join之后的where条件表达式
     *
     */
    private JoinFilterProcessor createJoinFilterProcessor()
        throws ExecutorException
    {
        if (joinOperator.getFilterAfterJoinExpression() == null)
        {
            return null;
        }
        
        IExpression filterExpression =
            new FilterViewExpressionCreator().create(getInputStream(),
                joinOperator.getFilterAfterJoinExpression(),
                applicationConfig);
        return new JoinFilterProcessor(filterExpression);
    }
    
    /**
     * 构建不同的join对象
     * join的条件也在其中
     * <p/>
     * 重要：所有的join，目前都不计算Rstream，都只计算IStream
     *
     */
    private IJoinComposer createJoinComposer()
        throws ExecutorException
    {
        switch (joinOperator.getJoinType())
        {
            case LEFT_OUTER_JOIN:
                return createSideBiJoinComposer(true);
            case RIGHT_OUTER_JOIN:
                return createSideBiJoinComposer(false);
            case FULL_OUTER_JOIN:
                return createFullOutBiJoinComposer();
            case INNER_JOIN:
                return createInnerBiJoinComposer();
            case CROSS_JOIN:
                return createCrossBiJoinComposer();
            default:
                ExecutorException exception =
                    new ExecutorException(ErrorCode.SEMANTICANALYZE_JOIN_UNSPPORTTED_NATURAL_JOIN);
                LOG.error("Unsupport natural join.", exception);
                throw exception;
        }
    }

    private IJoinComposer createCrossBiJoinComposer()
    {
        SimpleEventCollection left = new SimpleEventCollection(joinOperator.getLeftStreamName(), leftInputTupleEvent);
        SimpleEventCollection right =
            new SimpleEventCollection(joinOperator.getRightStreamName(), rightInputTupleEvent);
        return new CrossBiJoinComposer(left, right, isOutputRStreaming());
    }
    
    /**
     * 构建inner join表达式
     * IndexedMultiPropertyEventCollection的参数是一一对应的，比如 S1.id=S2.id,S1.name=s2.cname
     * 那么，left表达式中，表达式数组应该是 s1.id,s1.name
     * right表达式中，表达式数组应该是s2.id,s2.cname
     * <p/>
     * 表达式比如是propertyvalue表达式，目前不支持常量表达式或者 boolean表达式
     * <p/>
     * 其他不等值join，必须放在crossjoin中实现
     *
     */
    private IJoinComposer createInnerBiJoinComposer()
        throws ExecutorException
    {
        PropertyValueExpression[][] joinConditons = getJoinConditions();
        PropertyValueExpression[][] joinexps = revertJoinConditions(joinConditons);
        
        IndexedMultiPropertyEventCollection left =
            new IndexedMultiPropertyEventCollection(joinOperator.getLeftStreamName(), leftInputTupleEvent, joinexps[0]);
        IndexedMultiPropertyEventCollection right =
            new IndexedMultiPropertyEventCollection(joinOperator.getRightStreamName(), rightInputTupleEvent,
                joinexps[1]);
        return new InnerBiJoinComposer(left, right, isOutputRStreaming());
    }
    
    /**
     * 构建full outer join表达式
     * IndexedMultiPropertyEventCollection的参数是一一对应的，比如 S1.id=S2.id,S1.name=s2.cname
     * 那么，left表达式中，表达式数组应该是 s1.id,s1.name
     * right表达式中，表达式数组应该是s2.id,s2.cname
     * <p/>
     * 表达式比如是propertyvalue表达式，目前不支持常量表达式或者 boolean表达式
     * <p/>
     * 其他不等值join，必须放在crossjoin中实现
     *
     */
    private IJoinComposer createFullOutBiJoinComposer()
        throws ExecutorException
    {
        PropertyValueExpression[][] joinConditons = getJoinConditions();
        PropertyValueExpression[][] joinexps = revertJoinConditions(joinConditons);
        
        IndexedMultiPropertyEventCollection left =
            new IndexedMultiPropertyEventCollection(joinOperator.getLeftStreamName(), leftInputTupleEvent, joinexps[0]);
        IndexedMultiPropertyEventCollection right =
            new IndexedMultiPropertyEventCollection(joinOperator.getRightStreamName(), rightInputTupleEvent,
                joinexps[1]);
        return new FullOutBiJoinComposer(left, right, isOutputRStreaming());
    }
    
    /**
     * 构建inner join表达式
     * IndexedMultiPropertyEventCollection的参数是一一对应的，比如 S1.id=S2.id,S1.name=s2.cname
     * 那么，left表达式中，表达式数组应该是 s1.id,s1.name
     * right表达式中，表达式数组应该是s2.id,s2.cname
     * <p/>
     * 表达式比如是propertyvalue表达式，目前不支持常量表达式或者 boolean表达式
     * <p/>
     * 其他不等值join，必须放在crossjoin中实现
     *
     */
    private IJoinComposer createSideBiJoinComposer(boolean isLeftJoin)
        throws ExecutorException
    {
        SideJoinType joinType = isLeftJoin ? SideJoinType.LEFTJOIN : SideJoinType.RIGHTJOIN;
        PropertyValueExpression[][] joinConditons = getJoinConditions();
        PropertyValueExpression[][] joinexps = revertJoinConditions(joinConditons);
        
        IndexedMultiPropertyEventCollection left =
            new IndexedMultiPropertyEventCollection(joinOperator.getLeftStreamName(), leftInputTupleEvent, joinexps[0]);
        IndexedMultiPropertyEventCollection right =
            new IndexedMultiPropertyEventCollection(joinOperator.getRightStreamName(), rightInputTupleEvent,
                joinexps[1]);
        return new SideBiJoinComposer(left, right, joinType, isOutputRStreaming());
    }
    
    /**
     * 将本来集中在一起的join表达式分开表达式
     * <p/>
     * 本来数组是[[s1.id,s2.id],[s1.name,s2.cname]]
     * 改为
     * [[s1.id,s1.name],[s2.id,s2.cname]]
     *
     */
    private PropertyValueExpression[][] revertJoinConditions(PropertyValueExpression[][] joinConditons)
    {
        PropertyValueExpression[][] results =
            new PropertyValueExpression[JOINEXPRESSIONARRLENGHT][joinConditons.length];
        
        for (int i = 0; i < joinConditons.length; i++)
        {
            results[0][i] = joinConditons[i][0];
            results[1][i] = joinConditons[i][1];
        }
        return results;
    }
    
    private PropertyValueExpression[][] getJoinConditions()
        throws ExecutorException
    {
        
        List<Schema> inputSchemas = Lists.newArrayList();
        inputSchemas.addAll(cloneSchema(leftInputSchemas, leftTransitionIn.getStreamName()));
        inputSchemas.addAll(cloneSchema(rightInputSchemas, rightTransitionIn.getStreamName()));
        
        IExpression filterExpression =
            new FilterViewExpressionCreator().create(inputSchemas, joinOperator.getJoinExpression(), applicationConfig);
        
        List<PropertyValueExpression[]> results = Lists.newArrayList();
        
        parseJoinConditions(filterExpression, inputSchemas, results);
        
        resetIndexForSelftJoin(results);
        return results.toArray(new PropertyValueExpression[results.size()][JOINEXPRESSIONARRLENGHT]);
    }
    
    private void resetIndexForSelftJoin(List<PropertyValueExpression[]> results)
    {
        if (isSelfJoin())
        {
            for (PropertyValueExpression[] parr : results)
            {
                /*
                 * selfjoin的时候，join条件顺序无所谓，
                 * 只要保证不出现一个等式两边的propertyvalue表达式索引值一样即可。
                 */
                parr[1].setStreamIndex(1);
            }
        }
    }
    
    private void parseJoinConditions(IExpression expression, List<Schema> inputSchemas,
        List<PropertyValueExpression[]> results)
        throws ExecutorException
    {
        if (expression instanceof RelationExpression)
        {
            results.add(parseJoinLeftAndRightConditions((RelationExpression)expression));
            return;
        }
        
        if (!(expression instanceof LogicExpression))
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_UNSPPORTED_JOIN_CONDITION, expression.toString());
            LOG.error("Unsupport join condition, support logic expression.", exception);
            throw exception;
        }
        
        parseJoinConditions(((LogicExpression)expression).getLeftExpr(), inputSchemas, results);
        parseJoinConditions(((LogicExpression)expression).getRightExpr(), inputSchemas, results);
    }
    
    private PropertyValueExpression[] parseJoinLeftAndRightConditions(RelationExpression relation)
        throws ExecutorException
    {
        if (!relation.getOp().equals(ExpressionOperator.EQUAL))
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_UNSPPORTED_JOIN_CONDITION, "");
            LOG.error("Unsupport join condition, support equal.", exception);
            throw exception;
        }
        
        PropertyValueExpression[] exps = new PropertyValueExpression[JOINEXPRESSIONARRLENGHT];
        if (relation.getLeftExpr() instanceof PropertyValueExpression)
        {
            exps[0] = (PropertyValueExpression)relation.getLeftExpr();
        }
        else
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_UNSPPORTED_JOIN_CONDITION, "");
            LOG.error("Unsupport join condition, left expression must be property expression.", exception);
            throw exception;
        }
        
        if (relation.getRightExpr() instanceof PropertyValueExpression)
        {
            exps[1] = (PropertyValueExpression)relation.getRightExpr();
        }
        else
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_UNSPPORTED_JOIN_CONDITION, "");
            LOG.error("Unsupport join condition, right expression must be property expression.", exception);
            throw exception;
        }
        
        return exps;
    }
    
    /**
     * 克隆出一个新的schema出来并设置 流名称
     * <p/>
     * 由于流名称仅在多流汇聚的时候用到，其他时候不用，
     * 所以在对shcmea设置了流名称之后，用完了，还要重新置空，以避免对后面使用产生影响。
     * <p/>
     * 在这里，还是重新创建一个新的schema使用的好。
     *
     */
    private List<Schema> cloneSchema(List<Schema> schemas, String streamName)
        throws ExecutorException
    {
        List<Schema> result = Lists.newArrayList();
        for (int i = 0; i < schemas.size(); i++)
        {
            Schema s = schemas.get(i).cloneSchema();
            s.setStreamName(streamName);
            result.add(s);
        }
        return result;
    }
    
    private List<Schema> getInputStream()
    {
        List<Schema> schemas = Lists.newArrayList();
        schemas.addAll(this.leftInputSchemas);
        schemas.addAll(this.rightInputSchemas);
        return schemas;
    }
    
    private boolean isSelfJoin()
    {
        return joinOperator.getLeftStreamName().equals(joinOperator.getRightStreamName());
    }
    
    private OutputType getOutputType(Window leftWindow, Window rightWindow, UniDiRectionType unidirection)
        throws ExecutorException
    {
        //无需做window的空值判断。isRStreamWindow已经做了判断。
        //如果流两边都不包含R流输出的窗口，则直接输出I流
        if (!OutputTypeAnalyzer.isRStreamWindow(leftWindow) && !OutputTypeAnalyzer.isRStreamWindow(rightWindow))
        {
            return OutputType.I;
        }
        
        /*
         * 当单向输出的时候,如果R流窗口和单向的输出在同一侧，则可以正常输出，输出类型为R
         * 如果两侧都是R流窗口，并且没有单向输出，则输出类型为R
         */
        switch (unidirection)
        {
            case LEFT_STREAM:
                if (OutputTypeAnalyzer.isRStreamWindow(leftWindow))
                {
                    return OutputType.R;
                }
                return OutputType.I;
            case RIGHT_STREAM:
                if (OutputTypeAnalyzer.isRStreamWindow(rightWindow))
                {
                    return OutputType.R;
                }
                return OutputType.I;
            default:
                if (OutputTypeAnalyzer.isRStreamWindow(leftWindow) && OutputTypeAnalyzer.isRStreamWindow(rightWindow))
                {
                    return OutputType.R;
                }
                ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_UNSPPORTED_WINDOW_JOIN);
                LOG.error("RStream Window is not allowed in this join.", exception);
                throw exception;
        }
    }
    
    /**
     * 是否计算R流
     * 如果包含R流窗口，则计算，如果不包含，则不计算
     */
    private boolean isOutputRStreaming()
    {
        return this.outputType == OutputType.R;
    }
    
}
