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

package com.huawei.streaming.cql.executor;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.util.StreamingUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.AnnotationUtils;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.InnerInputSourceOperator;
import com.huawei.streaming.api.opereators.InnerOutputSourceOperator;
import com.huawei.streaming.api.opereators.InputStreamOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.OutputStreamOperator;
import com.huawei.streaming.api.opereators.serdes.SerDeAPI;
import com.huawei.streaming.api.opereators.serdes.UserDefinedSerDeAPI;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.ApplicationFactory;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorFactory;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.cql.semanticanalyzer.BaseAnalyzer;
import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.AbsOperator;
import com.huawei.streaming.operator.IRichOperator;
import com.huawei.streaming.operator.InputOperator;
import com.huawei.streaming.operator.OutputOperator;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * 执行计划生成器
 *
 */
public class ExecutorPlanGenerator
{
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorPlanGenerator.class);
    
    /**
     * 最终生成的，可以提交的应用程序
     */
    private com.huawei.streaming.application.Application executorApp;
    
    private Map<String, String> systemConfig = Maps.newHashMap();
    
    private Application apiApplication = null;
    
    /**
     * 生成执行计划
     * 主要内容包含：
     * 用户自定义的处理
     * 执行计划的组装
     * 表达式的解析
     * 构建application
     * <p/>
     * 主要步骤：
     * 1、解析所有的Schema，构建schema信息
     * 2、解析所有的Operator,构建OperatorInfo
     * 3、整理Operator中的上下级关系
     * 4、检查Application
     * 5、提交Application
     *
     */
    public com.huawei.streaming.application.Application generate(Application vap, DriverContext driverContext)
        throws ExecutorException
    {
        LOG.info("start to generator executor application for app " + vap.getApplicationId());
        apiApplication = vap;
        createEmptyApplication(vap.getApplicationId(),driverContext);
        parseUserDefineds(vap);
        parseSchemas();
        parseOperators();
        return executorApp;
    }
    
    private void parseUserDefineds(Application vap)
    {
        if (vap.getConfs() != null)
        {
            systemConfig.putAll(vap.getConfs());
        }
    }
    
    /**
     * 算子解析
     * <p/>
     * 这里的解析是为了使得输入和输出算子统一，
     * 避免用户自定义和系统内置的算子对外表现不一致处理起来的麻烦
     * <p/>
     * 由于输入和输出算子中存在特例，即针对文件，tcp，kafka等编写了特例
     * 所以需要首先将他们抽象化，之后再来处理
     * <p/>
     * 1、输入输出算子抽象化
     * 2、算子解析
     * 3、整理算子顺序
     * 4、添加算子到app
     *
     */
    private void parseOperators()
        throws ExecutorException
    {
        Map<String, Operator> opts = formatOperators();
        Map<String, AbsOperator> opMappings = createOperatorInfos(opts);
        combineOperators(opMappings);
        
        for (Entry<String, AbsOperator> et : opMappings.entrySet())
        {
            IRichOperator operator = et.getValue();
            //如果没有输入，也算是input
            if (operator instanceof InputOperator)
            {
                executorApp.addInputStream(operator);
                continue;
            }
            
            //如果没有输出，也算是output
            
            if (operator instanceof OutputOperator)
            {
                executorApp.addOutputStream(operator);
                continue;
            }
            
            executorApp.addFunctionStream(operator);
        }
    }
    
    private Map<String, Operator> formatOperators()
        throws ExecutorException
    {
        Map<String, Operator> inputs = formatInputSourceOperators();
        Map<String, Operator> outputs = formatOutputSourceOperators();
        return addOperatorsToNewList(inputs, outputs);
    }
    
    private Map<String, Operator> addOperatorsToNewList(Map<String, Operator> inputs, Map<String, Operator> outputs)
    {
        Map<String, Operator> opts = Maps.newHashMap();
        opts.putAll(inputs);
        opts.putAll(outputs);
        
        for (Operator operator : apiApplication.getOperators())
        {
            if (!opts.containsKey(operator.getId()))
            {
                opts.put(operator.getId(), operator);
            }
        }
        return opts;
    }
    
    /**
     * 识别系统输入算子并解析
     * 对于系统内置的输入算子，都必须属于BasicInputSourceOperator
     * 对于用户自定义的输入算子，都必须属于InputSourceOperator，所以要按照实例来识别
     *
     */
    private Map<String, Operator> formatInputSourceOperators()
        throws ExecutorException
    {
        Map<String, Operator> sourceOperators = Maps.newHashMap();
        for (Operator op : apiApplication.getOperators())
        {
            if (op instanceof InputStreamOperator)
            {
                sourceOperators.put(op.getId(), op);
                continue;
            }
            
            if (op instanceof InnerInputSourceOperator)
            {
                InputStreamOperator inputOperator = formatInnerInputStream(op);
                sourceOperators.put(inputOperator.getId(), inputOperator);
            }
        }
        
        return sourceOperators;
    }
    
    private InputStreamOperator formatInnerInputStream(Operator op)
        throws ApplicationBuildException
    {
        InputStreamOperator inputOperator = new InputStreamOperator(op.getId(), op.getParallelNumber());
        inputOperator.setArgs(op.getArgs());
        Class< ? extends SerDeAPI> deserClass = ((InnerInputSourceOperator)op).getDeserializer().getClass();
        Class< ? extends StreamSerDe> deserializerClass = AnnotationUtils.getStreamSerDeAnnoationOverClass(deserClass);
        String recordeReaderclass = InputOutputOperatorMapping.getPlatformOperatorByAPI(op.getClass().getName());
        
        if (deserializerClass != null)
        {
            inputOperator.setDeserializerClassName(deserializerClass.getName());
        }
        else
        {
            UserDefinedSerDeAPI userDeserializerAPI =
                (UserDefinedSerDeAPI)((InnerInputSourceOperator)op).getDeserializer();
            inputOperator.setDeserializerClassName(userDeserializerAPI.getSerDeClazz().getName());
            
        }
        inputOperator.setRecordReaderClassName(recordeReaderclass);
        
        if (inputOperator.getArgs() == null)
        {
            inputOperator.setArgs(new TreeMap<String, String>());
        }
        
        /*
         * 先加入系统全局参数，然后再添加CQL中的局部参数，
         * 保证局部参数覆盖全局参数
         */
        inputOperator.getArgs().putAll(systemConfig);
        
        Map<String, String> operatorConfig = AnnotationUtils.getAnnotationsToConfig(op);
        if (operatorConfig != null && !operatorConfig.isEmpty())
        {
            inputOperator.getArgs().putAll(operatorConfig);
        }
        
        Map<String, String> serdeConfig =
            AnnotationUtils.getAnnotationsToConfig(((InnerInputSourceOperator)op).getDeserializer());
        if (serdeConfig != null && !serdeConfig.isEmpty())
        {
            inputOperator.getArgs().putAll(serdeConfig);
        }
        return inputOperator;
    }
    
    /**
     * 识别系统输出算子并解析
     * 对于系统内置的输出算子，都必须属于BasicOutputSourceOperator
     * 对于用户自定义的输入算子，都必须属于InputSourceOperator，所以要按照实例来识别
     *
     */
    private Map<String, Operator> formatOutputSourceOperators()
        throws ApplicationBuildException
    {
        Map<String, Operator> sourceOperators = Maps.newHashMap();
        for (Operator op : apiApplication.getOperators())
        {
            if (op instanceof OutputStreamOperator)
            {
                sourceOperators.put(op.getId(), op);
                continue;
            }
            
            if (op instanceof InnerOutputSourceOperator)
            {
                OutputStreamOperator outputOperator = formatInnerOutputStream(op);
                sourceOperators.put(outputOperator.getId(), outputOperator);
            }
        }
        
        return sourceOperators;
    }
    
    private OutputStreamOperator formatInnerOutputStream(Operator op)
        throws ApplicationBuildException
    {
        OutputStreamOperator outputOperator = new OutputStreamOperator(op.getId(), op.getParallelNumber());
        outputOperator.setArgs(op.getArgs());
        
        Class< ? extends SerDeAPI> deserClass = ((InnerOutputSourceOperator)op).getSerializer().getClass();
        Class< ? extends StreamSerDe> serializerClass = AnnotationUtils.getStreamSerDeAnnoationOverClass(deserClass);
        String recordeWriter = InputOutputOperatorMapping.getPlatformOperatorByAPI(op.getClass().getName());
        
        if (serializerClass != null)
        {
            outputOperator.setSerializerClassName(serializerClass.getName());
        }
        else
        {
            UserDefinedSerDeAPI serializer = (UserDefinedSerDeAPI)((InnerOutputSourceOperator)op).getSerializer();
            outputOperator.setSerializerClassName(serializer.getSerDeClazz().getName());
        }
        outputOperator.setRecordWriterClassName(recordeWriter);
        
        if (outputOperator.getArgs() == null)
        {
            outputOperator.setArgs(new TreeMap<String, String>());
        }
        
        /*
         * 先加入系统全局参数，然后再添加CQL中的局部参数，
         * 保证局部参数覆盖全局参数
         */
        outputOperator.getArgs().putAll(systemConfig);
        
        Map<String, String> operatorConfig = AnnotationUtils.getAnnotationsToConfig(op);
        if (operatorConfig != null && !operatorConfig.isEmpty())
        {
            outputOperator.getArgs().putAll(operatorConfig);
        }
        
        Map<String, String> serdeConfig =
            AnnotationUtils.getAnnotationsToConfig(((InnerOutputSourceOperator)op).getSerializer());
        if (serdeConfig != null && !serdeConfig.isEmpty())
        {
            outputOperator.getArgs().putAll(serdeConfig);
        }
        return outputOperator;
    }
    
    /**
     * 梳理operatorInfo之间的上下级关系
     * <p/>
     * 这里有一个难题，上下级关系只能 set一次，如果重复set，application就会报错。
     * 所以，这里要排除重复，而且还要防止xml中的时序本来就是重复的。
     * <p/>
     * 由于底层API的原因，在设置上下级关系的时候，既要通过setinput和setoutput的API来设置
     * 还要通过setconfig的方式来设置，后面考虑进行规整。
     *
     */
    private void combineOperators(Map<String, AbsOperator> operatorInfos)
        throws ExecutorException
    {
        if (apiApplication.getOpTransition() == null)
        {
            return;
        }
        for (OperatorTransition ot : apiApplication.getOpTransition())
        {
            String fromOpId = ot.getFromOperatorId();
            String toOpId = ot.getToOperatorId();
            String streamName = ot.getStreamName();
            DistributeType distributedType = ot.getDistributedType();
            String distributedFields = ot.getDistributedFields();
            String outputSchemaName = ot.getSchemaName();
            
            if (!StringUtils.isEmpty(distributedFields))
            {
                distributedFields = distributedFields.toLowerCase(Locale.US);
            }
            
            distributedFields = ExecutorUtils.removeStreamName(distributedFields);
            
            TupleEventType outputSchema = (TupleEventType)(executorApp.getEventType(outputSchemaName));
            
            combineFromTransition(operatorInfos, fromOpId, streamName, outputSchema);
            combineToTransition(operatorInfos, toOpId, streamName, distributedType, distributedFields, outputSchema);
        }
    }
    
    private void combineToTransition(Map<String, AbsOperator> operatorInfos, String toOpId, String streamName,
        DistributeType distributedType, String distributedFields, TupleEventType outputSchema)
        throws ExecutorException
    {
        AbsOperator fopInfo = operatorInfos.get(toOpId);
        
        if (!StringUtils.isEmpty(distributedFields))
        {
            fopInfo.setGroupInfo(streamName, distributedType, distributedFields.split(","));
        }
        else
        {
            fopInfo.setGroupInfo(streamName, distributedType, null);
        }
        StreamingConfig sConfig = fopInfo.getConfig();
        sConfig = (sConfig == null ? new StreamingConfig() : sConfig);
        sConfig.put(StreamingConfig.STREAMING_INNER_INPUT_STREAM_NAME, streamName);
        sConfig.put(StreamingConfig.STREAMING_INNER_INPUT_SCHEMA, StreamingUtils.serializeSchema(outputSchema));
        try
        {
            fopInfo.setConfig(sConfig);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    private void combineFromTransition(Map<String, AbsOperator> operatorInfos, String fromOpId, String streamName,
        TupleEventType outputSchema)
        throws ExecutorException
    {
        AbsOperator sopInfo = operatorInfos.get(fromOpId);
        StreamingConfig sConfig = sopInfo.getConfig();
        sConfig = (sConfig == null ? new StreamingConfig() : sConfig);
        sConfig.put(StreamingConfig.STREAMING_INNER_OUTPUT_SCHEMA, StreamingUtils.serializeSchema(outputSchema));
        sConfig.put(StreamingConfig.STREAMING_INNER_OUTPUT_STREAM_NAME, streamName);
        try
        {
            sopInfo.setConfig(sConfig);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    /**
     * 解析功能性算子信息
     *
     */
    private Map<String, AbsOperator> createOperatorInfos(Map<String, Operator> operators)
        throws ExecutorException
    {
        Map<String, AbsOperator> opMappings = Maps.newHashMap();
        
        for (Operator op : operators.values())
        {
            AbsOperator opinfo = createOperatorInfo(op);
            opMappings.put(opinfo.getOperatorId(), opinfo);
        }
        return opMappings;
    }
    
    /**
     * 进行功能性算子解析
     * <p/>
     * 这些算子中包含了joinOperator，FunctionOperator等算子
     * 但是不包含input和output算子
     *
     */
    private AbsOperator createOperatorInfo(Operator operator)
        throws ExecutorException
    {
        return OperatorInfoCreatorFactory.createOperatorInfo(apiApplication,
            operator,
            executorApp.getStreamSchema(),
            this.systemConfig);
    }
    
    /**
     * 解析schema信息
     * 规整schema的名称，列名称全部使用小写，schema名称也全部小写
     *
     */
    private void parseSchemas()
        throws ExecutorException
    {
        BaseAnalyzer.setSchemaNameInAttributes(apiApplication.getSchemas());
        for (Schema schema : apiApplication.getSchemas())
        {
            TupleEventType tupleEventType = parseSchemaToIEvent(schema);
            try
            {
                executorApp.addEventSchema(tupleEventType);
            }
            catch (StreamingException e)
            {
                throw ExecutorException.wrapStreamingException(e);
            }
        }
    }
    
    /**
     * 将schema转为IEvent事件
     *
     */
    private TupleEventType parseSchemaToIEvent(Schema schema)
        throws ExecutorException
    {
        List<Attribute> attrs = Lists.newArrayList();
        for (int i = 0; i < schema.getCols().size(); i++)
        {
            Class< ? > type = getColumnDataType(schema, i);
            String colName = schema.getCols().get(i).getName();
            attrs.add(new Attribute(type, colName));
        }
        
        return new TupleEventType(schema.getId(), attrs);
    }
    
    private Class< ? > getColumnDataType(Schema schema, int i)
        throws ExecutorException
    {
        try
        {
            return Class.forName(schema.getCols().get(i).getType(), true, CQLUtils.getClassLoader());
        }
        catch (ClassNotFoundException e)
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_UNSUPPORTED_DATATYPE, schema.getCols().get(i).getType());
            LOG.error("Unsupport data type.", exception);
            
            throw exception;
        }
    }
    
    private void createEmptyApplication(String appid, DriverContext driverContext)
        throws ExecutorException
    {
        Map<String, String> appConf = apiApplication.getConfs();
        StreamingConfig applicationConfig = getApplicationConfig(appConf);
        try
        {
            executorApp = ApplicationFactory.createApplication(driverContext,applicationConfig, appid);
        }
        catch (StreamingException e)
        {
            ExecutorException.wrapStreamingException(e);
        }
    }
    
    private StreamingConfig getApplicationConfig(Map<String, String> applicationConfig)
    {
        StreamingConfig conf = new StreamingConfig();
        if (applicationConfig != null)
        {
            conf.putAll(applicationConfig);
        }
        return conf;
    }
}
