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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.huawei.streaming.api.opereators.*;
import com.huawei.streaming.util.StreamingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.AnnotationUtils;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.exception.ParseException;
import com.huawei.streaming.cql.executor.operatorviewscreater.AggResultSetMergeViewCreator;
import com.huawei.streaming.cql.executor.operatorviewscreater.AggResultSetParameters;
import com.huawei.streaming.cql.executor.operatorviewscreater.FilterViewExpressionCreator;
import com.huawei.streaming.cql.executor.operatorviewscreater.WindowViewCreator;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.DataSourceBodyContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionContext;
import com.huawei.streaming.datasource.DataSourceContainer;
import com.huawei.streaming.datasource.IDataSource;
import com.huawei.streaming.datasource.PreStatementRDBDataSource;
import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.operator.AbsOperator;
import com.huawei.streaming.operator.FunctionOperator;
import com.huawei.streaming.operator.functionstream.DataSourceFunctionOp;
import com.huawei.streaming.process.agg.resultmerge.IAggResultSetMerge;
import com.huawei.streaming.process.join.CrossBiJoinComposer;
import com.huawei.streaming.process.join.IJoinComposer;
import com.huawei.streaming.process.join.JoinFilterProcessor;
import com.huawei.streaming.process.join.SimpleEventCollection;
import com.huawei.streaming.window.IWindow;

/**
 * DataSource Info Creator
 *
 */
public class DataSourceInfoOperatorCreator implements OperatorInfoCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceInfoOperatorCreator.class);
    
    private BaseDataSourceOperator dataSourceOperator;
    
    private OperatorTransition leftTransitionIn = null;
    
    private OperatorTransition transitionOut = null;
    
    private List<Schema> leftInputSchemas;
    
    private List<Schema> rightInputSchemas;
    
    private List<Schema> outputSchemas;
    
    private IEventType leftInputTupleEvent = null;
    
    private IEventType rightInputTupleEvent = null;
    
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
        
        IWindow leftWindow =
            new WindowViewCreator().create(leftInputSchemas, dataSourceOperator.getLeftWindow(), systemConfig);
        
        JoinFilterProcessor jfProcessor = createJoinFilterProcessor();
        
        IExpression filterExp = null;
        if (jfProcessor != null)
        {
            filterExp = jfProcessor.getExpr();
        }
        
        AggResultSetParameters pars = createResultSetMergeParmeters(streamschema, leftWindow, filterExp);
        IAggResultSetMerge joinProcessor = new AggResultSetMergeViewCreator(pars).create();
        
        DataSourceContainer dataSource = createDataSourceInstance();
        
        FunctionOperator joinop =
            new DataSourceFunctionOp(leftWindow, dataSource, createDataSourceCQLExpressions(dataSource),
                createJoinComposer(), jfProcessor, joinProcessor,
                OutputTypeAnalyzer.createOutputType(dataSourceOperator.getLeftWindow()));
        
        StreamingConfig config = new StreamingConfig();
        config.putAll(this.applicationConfig);
        setUniDirectionConfig(config);
        setJoinConfig(config);
        joinop.setConfig(config);
        return OperatorInfoCreatorFactory.buildStreamOperator(operator, joinop);
    }
    
    private Map<String, IExpression> createDataSourceCQLExpressions(DataSourceContainer dataSource)
        throws ExecutorException
    {
        Map<String, IExpression> cqlArgs = Maps.newHashMap();
        String[] args = dataSource.getCQLQueryArguments();
        for (String arg : args)
        {
            IExpression filterExpression =
                new FilterViewExpressionCreator().create(leftInputSchemas, arg, applicationConfig);
            cqlArgs.put(arg, filterExpression);
        }
        return cqlArgs;
    }
    
    private DataSourceContainer createDataSourceInstance()
        throws ExecutorException
    {
        /*
         * 如果不是系统内置的rdb数据源，则按照用户自定义方式处理
         */
        if (dataSourceOperator instanceof RDBDataSourceOperator)
        {
            DataSourceOperator commonDataSource = createRDBDataSource(dataSourceOperator);
            setDataSourceConfig(commonDataSource);
            commonDataSource.setDataSourceClassName(PreStatementRDBDataSource.class.getName());
            return convertCommonDataSource(commonDataSource);
        }
        else
        {
            return convertCommonDataSource((DataSourceOperator)dataSourceOperator);
            
        }
    }


    private void setDataSourceConfig(DataSourceOperator commonDataSource)
        throws ApplicationBuildException
    {
        TreeMap<String, String> config = AnnotationUtils.getAnnotationsToConfig(dataSourceOperator);
        commonDataSource.setDataSourceConfig(config);
    }
    
    private DataSourceOperator createRDBDataSource(BaseDataSourceOperator baseDataSource)
    {
        DataSourceOperator dataSource =
            new DataSourceOperator(baseDataSource.getId(), baseDataSource.getParallelNumber());
        dataSource.setName(baseDataSource.getName());
        dataSource.setArgs(baseDataSource.getArgs());
        dataSource.setLeftStreamName(baseDataSource.getLeftStreamName());
        dataSource.setLeftWindow(baseDataSource.getLeftWindow());
        dataSource.setFilterAfterJoinExpression(baseDataSource.getFilterAfterJoinExpression());
        dataSource.setQueryArguments(baseDataSource.getQueryArguments());
        dataSource.setDataSourceSchema(baseDataSource.getDataSourceSchema());
        dataSource.setFilterAfterAggregate(baseDataSource.getFilterAfterAggregate());
        dataSource.setGroupbyExpression(baseDataSource.getGroupbyExpression());
        baseDataSource.setOrderBy(baseDataSource.getOrderBy());
        baseDataSource.setLimit(baseDataSource.getLimit());
        dataSource.setOutputExpression(baseDataSource.getOutputExpression());
        return dataSource;
    }


    private DataSourceOperator createRedisStringDataSource(BaseDataSourceOperator baseDataSource) {
        DataSourceOperator dataSource =
                new DataSourceOperator(baseDataSource.getId(), baseDataSource.getParallelNumber());

        dataSource.setName(baseDataSource.getName());
        dataSource.setArgs(baseDataSource.getArgs());
        dataSource.setLeftStreamName(baseDataSource.getLeftStreamName());
        dataSource.setLeftWindow(baseDataSource.getLeftWindow());
        dataSource.setFilterAfterJoinExpression(baseDataSource.getFilterAfterJoinExpression());
        dataSource.setQueryArguments(baseDataSource.getQueryArguments());
        dataSource.setDataSourceSchema(baseDataSource.getDataSourceSchema());
        dataSource.setFilterAfterAggregate(baseDataSource.getFilterAfterAggregate());
        dataSource.setGroupbyExpression(baseDataSource.getGroupbyExpression());
        baseDataSource.setOrderBy(baseDataSource.getOrderBy());
        baseDataSource.setLimit(baseDataSource.getLimit());
        dataSource.setOutputExpression(baseDataSource.getOutputExpression());
        return dataSource;

    }
    
    private DataSourceContainer convertCommonDataSource(DataSourceOperator commonDataSource)
        throws ExecutorException
    {
        DataSourceContainer dataSource = new DataSourceContainer();
        dataSource.setSchema((TupleEventType)rightInputTupleEvent);
        dataSource.setDataSource(createIDataSource(commonDataSource));
        try
        {
            dataSource.setQueryArguments(dataSourceQueryArguments());
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
        return dataSource;
    }

    private IDataSource createIDataSource(DataSourceOperator commonDataSource) throws ExecutorException
    {
        IDataSource dataSource = createIDataSourceInstance(commonDataSource);

        StreamingConfig config = new StreamingConfig();
        config.putAll(applicationConfig);
        if (commonDataSource.getDataSourceConfig() != null)
        {
            config.putAll(commonDataSource.getDataSourceConfig());
        }

        try
        {
            dataSource.setConfig(config);
            dataSource.setSchema((TupleEventType)rightInputTupleEvent);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
        return dataSource;
    }

    private IDataSource createIDataSourceInstance(DataSourceOperator commonDataSource) throws ApplicationBuildException
    {
        String dataSourceClassName = commonDataSource.getDataSourceClassName();
        IDataSource dataSource = null;
        try
        {
            Class< ? > dataSourceClazz = Class.forName(dataSourceClassName, true, CQLUtils.getClassLoader());
            dataSource =  (IDataSource)dataSourceClazz.newInstance();
        }
        catch (ReflectiveOperationException e)
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_DATASOURCE_UNKNOWN, dataSourceClassName);
            LOG.error("Unknown dataSource, reflective operation error.", exception);
            throw exception;
            
        }
        return dataSource;
    }

    private String[] dataSourceQueryArguments()
        throws ExecutorException
    {
        List<String> args = dataSourceOperator.getQueryArguments();
        String argStr = Joiner.on(", ").join(args);
        IParser parser = ParserFactory.createDataSourceArgumentsParser();
        try
        {
            if(Strings.isNullOrEmpty(argStr))
            {
                return new String[]{""};
            }
            DataSourceBodyContext dataSourceBodyContext = (DataSourceBodyContext)parser.parse(argStr);
            List<ExpressionContext > argLists = dataSourceBodyContext.getQueryarguments();
            String[] results = new String[argLists.size()];
            for (int i = 0; i < results.length; i++)
            {
                results[i] = argLists.get(i).toString();
            }
            return results;
        }
        catch (ParseException e)
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_PARSE_ERROR, argStr);
            LOG.error("Data source semantic analyze error.", exception);
            throw exception;
        }
    }
    
    private void prepare(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemconfig)
        throws ExecutorException
    {
        this.applicationConfig = systemconfig;
        this.dataSourceOperator = (BaseDataSourceOperator)operator;
        this.leftTransitionIn =
            OperatorInfoCreatorFactory.getTransitionIn(vapp, operator, dataSourceOperator.getLeftStreamName());
        
        this.transitionOut = OperatorInfoCreatorFactory.getTransitionOut(vapp, operator);
        this.leftInputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, leftTransitionIn);
        this.rightInputSchemas = Lists.newArrayList(dataSourceOperator.getDataSourceSchema());
        this.outputSchemas = OperatorInfoCreatorFactory.getSchemasByTransition(vapp, transitionOut);
        this.leftInputTupleEvent = streamschema.getEventType(leftTransitionIn.getSchemaName());
        this.rightInputTupleEvent = parseSchemaToIEvent(dataSourceOperator.getDataSourceSchema());
    }
    
    /**
     * 将schema转为IEvent事件
     *
     */
    private TupleEventType parseSchemaToIEvent(Schema schema)
        throws ExecutorException
    {
        List<Attribute> attrs = new ArrayList<Attribute>();
        for (int i = 0; i < schema.getCols().size(); i++)
        {
            Class< ? > type = null;
            String clazz = schema.getCols().get(i).getType();
            try
            {
                type = Class.forName(clazz, true, CQLUtils.getClassLoader());
            }
            catch (ClassNotFoundException e)
            {
                ApplicationBuildException exception =
                    new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNSUPPORTED_DATATYPE, clazz);
                LOG.error("Unsupport data type.", exception);
                throw exception;
            }
            String colName = schema.getCols().get(i).getName();
            attrs.add(new Attribute(type, colName));
        }
        
        return new TupleEventType(schema.getId(), attrs);
    }
    
    private AggResultSetParameters createResultSetMergeParmeters(EventTypeMng streamschema, IWindow leftWindow,
        IExpression whereExpression)
        throws ExecutorException
    {
        /*
         * 这里的list中的schema的顺序不能乱，第一个一定是left，第二个一定是right
         * 因为后面select表达式中需要使用
         */
        List<Schema> inputSchemas = new ArrayList<Schema>();
        inputSchemas.addAll(cloneSchema(leftInputSchemas, leftTransitionIn.getStreamName()));
        inputSchemas.addAll(cloneSchema(rightInputSchemas, rightInputTupleEvent.getEventTypeName()));
        
        Map<String, IWindow> streamWindows = new HashMap<String, IWindow>();
        streamWindows.put(leftInputSchemas.get(0).getStreamName(), leftWindow);
        
        List<Window> operatorWindows = Lists.newArrayList();
        operatorWindows.add(dataSourceOperator.getLeftWindow());
        
        AggResultSetParameters pars = new AggResultSetParameters();
        pars.setBasicAggOperator(dataSourceOperator);
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
    
    private void setJoinConfig(StreamingConfig config)
    {
        config.put(StreamingConfig.OPERATOR_JOIN_INNER_LEFT_INPUT_STREAM_NAME, dataSourceOperator.getLeftStreamName());
        config.put(StreamingConfig.OPERATOR_JOIN_INNER_LEFT_SCHEMA, StreamingUtils.serializeSchema((TupleEventType) leftInputTupleEvent));
    }
    
    private void setUniDirectionConfig(StreamingConfig config)
    {
        config.put(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL, true);
        config.put(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL_INDEX, 0);
        config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL, true);
        config.put(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL_INDEX, 0);
    }
    
    /**
     * join之后的where条件表达式
     *
     */
    private JoinFilterProcessor createJoinFilterProcessor()
        throws ExecutorException
    {
        if (dataSourceOperator.getFilterAfterJoinExpression() == null)
        {
            return null;
        }
        
        IExpression filterExpression =
            new FilterViewExpressionCreator().create(getInputStream(),
                dataSourceOperator.getFilterAfterJoinExpression(),
                applicationConfig);
        return new JoinFilterProcessor(filterExpression);
    }
    
    private List<Schema> getInputStream()
    {
        List<Schema> schemas = Lists.newArrayList();
        schemas.addAll(this.leftInputSchemas);
        schemas.addAll(this.rightInputSchemas);
        return schemas;
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
        return createCrossBiJoinComposer();
    }
    
    private IJoinComposer createCrossBiJoinComposer()
    {
        SimpleEventCollection left =
            new SimpleEventCollection(dataSourceOperator.getLeftStreamName(), leftInputTupleEvent);
        SimpleEventCollection right =
            new SimpleEventCollection(rightInputTupleEvent.getEventTypeName(), rightInputTupleEvent);
        return new CrossBiJoinComposer(left, right, false);
    }
}
