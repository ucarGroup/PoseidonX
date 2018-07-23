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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.huawei.streaming.api.AnnotationUtils;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.mapping.SimpleLexer;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ClassNameContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ColumnNameTypeListContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.CreateOutputStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 创建输出流语义分析
 *
 */
public class CreateOutputStreamAnalyzer extends CreateStreamAnalyzer
{
    private static final Logger LOG = LoggerFactory.getLogger(CreateInputStreamAnalyzer.class);
    
    private CreateOutputStatementContext createOutputStreamParseContext = null;
    
    /**
     * <默认构造函数>
     *
     */
    public CreateOutputStreamAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        createOutputStreamParseContext = (CreateOutputStatementContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        String streamName = createOutputStreamParseContext.getStreamName();
        ColumnNameTypeListContext columns = createOutputStreamParseContext.getColumns();
        
        getAnalyzeContext().setStreamName(streamName);
        getAnalyzeContext().setSchema(createSchema(streamName, columns));
        
        setSerDeDefine();
        setSinkDefine();
        setParallelNumber();
        return getAnalyzeContext();
    }
    
    private void setSerDeDefine()
        throws SemanticAnalyzerException
    {
        setSerDeClass();
        setSerDeProperties();
    }
    
    private void setSerDeClass()
        throws SemanticAnalyzerException
    {
        ClassNameContext serClassName = createOutputStreamParseContext.getSerClassName();
        if (serClassName == null)
        {
            setSerDeByDefault();
        }
        else
        {
            setSerDeByCQL(serClassName);
        }
    }
    
    private void setSerDeByCQL(ClassNameContext serClassName)
        throws SemanticAnalyzerException
    {
        String newSerClassName = serClassName.getClassName();
        
        if (serClassName.isInnerClass())
        {
           String fullName = SimpleLexer.SERDE.getFullName(newSerClassName);
            if(fullName == null)
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_UNMATCH_OPERATOR, newSerClassName);
                LOG.error("The '{}' operator type does not match.", newSerClassName);
                throw exception;
            }
            newSerClassName = fullName;
        }
        getAnalyzeContext().setSerializerClassName(newSerClassName);
    }
    
    private void setSerDeByDefault()
        throws SemanticAnalyzerException
    {
        StreamingConfig conf = new StreamingConfig();
        if (conf.containsKey(StreamingConfig.STREAMING_SERDE_DEFAULT))
        {
            getAnalyzeContext().setSerializerClassName((String)conf.get(StreamingConfig.STREAMING_SERDE_DEFAULT));
        }
        else
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_UNKNOWN_SERDE);
            LOG.error("Failed to set default serializer.", exception);
            
            throw exception;
        }
    }
    
    private void setSerDeProperties()
        throws SemanticAnalyzerException
    {
        Map<String, String> serdeProperties =
            analyzeStreamProperties(createOutputStreamParseContext.getSerProperties());
        getAnalyzeContext().setSerDeProperties(convertSerDeSimpleConf(serdeProperties));
    }
    
    private Map<String, String> convertSerDeSimpleConf(final Map<String, String> serdeConf)
        throws SemanticAnalyzerException
    {
        String serdeClassName = getAnalyzeContext().getSerializerClassName();
        return convertSimpleConf(serdeConf, serdeClassName);
    }
    
    private Map<String, String> convertSinkSimpleConf(final Map<String, String> sinkConf)
        throws SemanticAnalyzerException
    {
        String sinkClassName = getAnalyzeContext().getRecordWriterClassName();
        return convertSimpleConf(sinkConf, sinkClassName);
    }

    private Map<String, String> convertSimpleConf(Map<String, String> serdeProperties, String deserClassName)
        throws SemanticAnalyzerException
    {
        String apiOperator = InputOutputOperatorMapping.getAPIOperatorByPlatform(deserClassName);
        if (apiOperator == null)
        {
            return serdeProperties;
        }

        Map<String, String> configMapping = AnnotationUtils.getConfigMapping(apiOperator);
        Map<String, String> confs = Maps.newHashMap();

        for (Map.Entry<String, String> et : serdeProperties.entrySet())
        {
            String fullName = et.getKey();
            String value = et.getValue();
            //大小写完全匹配
            if (configMapping.containsKey(fullName))
            {
                fullName = configMapping.get(fullName);
            }
            confs.put(fullName, value);
        }

        return confs;
    }
    
    private void setSinkDefine()
        throws SemanticAnalyzerException
    {
        setSinkClass();
        setSinkProperties();
    }
    
    private void setSinkClass()
        throws SemanticAnalyzerException
    {
        ClassNameContext sinkClassName = createOutputStreamParseContext.getSinkClassName();
        String newSinkClassName = sinkClassName.getClassName();
        if (sinkClassName.isInnerClass())
        {
            String fullName = SimpleLexer.OUTPUT.getFullName(newSinkClassName);
            if (fullName == null)
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_UNMATCH_OPERATOR, newSinkClassName);
                LOG.error("The '{}' operator type does not match.", newSinkClassName);
                throw exception;
            }
            newSinkClassName = fullName;
        }

        getAnalyzeContext().setRecordWriterClassName(newSinkClassName);
    }
    
    private void setSinkProperties()
        throws SemanticAnalyzerException
    {
        Map<String, String> sinkProperties =
            analyzeStreamProperties(createOutputStreamParseContext.getSinkProperties());
        getAnalyzeContext().setReadWriterProperties(convertSinkSimpleConf(sinkProperties));
    }
    
    private void setParallelNumber()
        throws SemanticAnalyzerException
    {
        if (createOutputStreamParseContext.getParallelNumber() != null)
        {
            String number = createOutputStreamParseContext.getParallelNumber().getNumber();
            Integer parallelNumber = ConstUtils.formatInt(number);
            getAnalyzeContext().setParallelNumber(parallelNumber);
        }
    }
    
}
