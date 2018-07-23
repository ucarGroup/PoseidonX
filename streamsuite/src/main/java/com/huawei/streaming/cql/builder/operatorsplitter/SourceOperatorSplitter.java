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

package com.huawei.streaming.cql.builder.operatorsplitter;

import java.util.TreeMap;

import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.opereators.InputStreamOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OutputStreamOperator;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.builder.operatorconverter.InputConverter;
import com.huawei.streaming.cql.builder.operatorconverter.OutputConverter;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.mapping.SimpleLexer;
import com.huawei.streaming.cql.semanticanalyzer.BaseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.CreateStreamAnalyzeContext;

/**
 * input和 output 算子的拆分
 * 
 */
public class SourceOperatorSplitter implements Splitter
{
    private CreateStreamAnalyzeContext context;
    
    private SplitContext result = new SplitContext();
    
    private InputConverter inputConverter = new InputConverter();
    
    private OutputConverter outputConverter = new OutputConverter();
    
    private BuilderUtils buildUtils;
    
    private int parallelNumber;
    
    /**
     * <默认构造函数>
     */
    public SourceOperatorSplitter(BuilderUtils buildUtils)
    {
        this.buildUtils = buildUtils;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(AnalyzeContext parseContext)
    {
        return parseContext instanceof CreateStreamAnalyzeContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SplitContext split(AnalyzeContext parseContext)
        throws ApplicationBuildException
    {
        context = (CreateStreamAnalyzeContext)parseContext;
        setParallelNumber();
        addToInput();
        addToOutput();
        addToPipe();
        result.setParseContext(context);
        return result;
    }
    
    private OutputStreamOperator createOutputOperator()
    {
        String operatorName = getOutputOperatorName(context.getRecordWriterClassName(),"Output");
        OutputStreamOperator op = new OutputStreamOperator(buildUtils.getNextOperatorName(operatorName), parallelNumber);
        op.setName(context.getStreamAlias());
        op.setSerializerClassName(context.getSerializerClassName());
        op.setRecordWriterClassName(context.getRecordWriterClassName());
        op.setArgs(new TreeMap<String, String>());
        op.getArgs().putAll(context.getReadWriterProperties());
        op.getArgs().putAll(context.getSerDeProperties());
        return op;
    }
    
    private InputStreamOperator createInputSourceOperator()
    {
        String operatorName = getInputOperatorName(context.getRecordReaderClassName(),"Input");
        InputStreamOperator op = new InputStreamOperator(buildUtils.getNextOperatorName(operatorName), parallelNumber);
        op.setName(context.getStreamAlias());
        op.setDeserializerClassName(context.getDeserializerClassName());
        op.setRecordReaderClassName(context.getRecordReaderClassName());
        op.setArgs(new TreeMap<String, String>());
        op.getArgs().putAll(context.getReadWriterProperties());
        op.getArgs().putAll(context.getSerDeProperties());
        return op;
    }

    private String getInputOperatorName(String operatorClassName, String defaultName)
    {
        if (operatorClassName == null)
        {
            return defaultName;
        }

        String simpleName = SimpleLexer.INPUT.getSimpleName(operatorClassName);
        return simpleName == null ? defaultName : simpleName;
    }

    private String getOutputOperatorName(String operatorClassName, String defaultName)
    {
        if (operatorClassName == null)
        {
            return defaultName;
        }

        String simpleName = SimpleLexer.OUTPUT.getSimpleName(operatorClassName);
        return simpleName == null ? defaultName : simpleName;
    }

    /**
     * 创建pipe stream算子
     * 
     * pipe stream算子属于中间算子，本来是不会对应任何算子，只要解析出schema即可的
     * 但是，为了和CQL的整体规则一直，便于后面创建算子之间的连线，
     * 所以，这里创建一个空的filter算子，不带任何过滤。
     * 这样，就可以在优化器阶段将这个filter算子优化掉
     */
    private Operator createPipeSourceOperator()
        throws ApplicationBuildException
    {
        FilterOperator fop = new FilterOperator(buildUtils.getNextOperatorName("Filter"), parallelNumber);
        fop.setOutputExpression(createFilterOutputExpression(context.getStreamName()));
        fop.setName(context.getStreamAlias());
        return fop;
    }
    
    private String createFilterOutputExpression(String streamName)
        throws SemanticAnalyzerException
    {
        Schema schema = BaseAnalyzer.getSchemaByName(streamName, context.getCreatedSchemas());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schema.getCols().size(); i++)
        {
            sb.append(streamName + "." + schema.getCols().get(i).getName());
            if (i != schema.getCols().size() - 1)
            {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    private void addToPipe()
        throws ApplicationBuildException
    {
        if (context.getDeserializerClassName() == null && context.getSerializerClassName() == null)
        {
            Operator pop = createPipeSourceOperator();
            result.addOperators(pop);
        }
    }
    
    private void addToOutput()
        throws ApplicationBuildException
    {
        if (context.getDeserializerClassName() == null && context.getSerializerClassName() != null)
        {
            
            Operator inop = createOutputOperator();
            if (outputConverter.validate(inop))
            {
                inop = outputConverter.convert(inop);
            }
            result.addOperators(inop);
        }
    }
    
    private void addToInput()
        throws ApplicationBuildException
    {
        if (context.getDeserializerClassName() != null && context.getSerializerClassName() == null)
        {
            Operator inop = createInputSourceOperator();
            if (inputConverter.validate(inop))
            {
                inop = inputConverter.convert(inop);
            }
            result.addOperators(inop);
        }
    }
    
    private void setParallelNumber()
    {
        if (context.getParallelNumber() == null)
        {
            parallelNumber = buildUtils.getDefaultParallelNumber();
        }
        else
        {
            parallelNumber = context.getParallelNumber();
        }
    }
    
}
