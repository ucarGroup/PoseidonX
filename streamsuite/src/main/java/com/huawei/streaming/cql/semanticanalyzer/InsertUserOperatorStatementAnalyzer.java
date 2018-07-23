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

import java.util.TreeMap;

import com.google.common.collect.Maps;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.InsertUserOperatorStatementAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ColumnNameTypeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ColumnNameTypeListContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.CreateOperatorContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FullUserOperatorContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.InsertUserOperatorStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.KeyValuePropertyContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.StreamPropertiesContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.UsingStatementContext;

/**
 * 使用自定义算子 语义分析
 */
public class InsertUserOperatorStatementAnalyzer extends BaseAnalyzer
{
    
    private InsertUserOperatorStatementAnalyzeContext insertUserOperatorStatementAnalyzeContext;
    
    private FullUserOperatorContext fullContext;
    
    /**
     * insert用户自定义算子语义分析
     */
    public InsertUserOperatorStatementAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        fullContext = (FullUserOperatorContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        initInsertContext();
        initCreateContext();
        
        if (!checkSchemaExists(getAnalyzeContext().getOutputStreamName()))
        {
            getAnalyzeContext().setPipeStreamNotCreated(true);
        }
        
        return getAnalyzeContext();
    }
    
    private void initCreateContext()
    {
        CreateOperatorContext createContext = fullContext.getCreateContext();
        getAnalyzeContext().setOperatorClassName(createContext.getOperatorClassName().getClassName());
        getAnalyzeContext().setInputSchmea(createSchema(getAnalyzeContext().getInputStreamName(),
            createContext.getInputSchema().getInputSchema()));
        getAnalyzeContext().setOutputSchema(createSchema(getAnalyzeContext().getOutputStreamName(),
            createContext.getOutputSchema().getOutputSchema()));
        getAnalyzeContext().setProperties(analyzeStreamProperties(createContext.getOperatorProperties()));
    }
    
    private Schema createSchema(String streamName, ColumnNameTypeListContext columns)
    {
        Schema schema = new Schema(streamName);
        for (ColumnNameTypeContext column : columns.getColumns())
        {
            schema.addCol(new Column(column.getColumnName(), column.getColumnType().getWrapperClass()));
        }
        return schema;
    }
    
    private TreeMap<String, String> analyzeStreamProperties(StreamPropertiesContext streamPropertiesContext)
    {
        if (streamPropertiesContext == null)
        {
            return Maps.newTreeMap();
        }
        
        return parseStreamProperties(streamPropertiesContext);
    }
    
    private TreeMap<String, String> parseStreamProperties(StreamPropertiesContext streamPropertiesContext)
    {
        TreeMap<String, String> properties = Maps.newTreeMap();
        for (KeyValuePropertyContext ctx : streamPropertiesContext.getProperties())
        {
            String key = ctx.getKey();
            String value = ctx.getValue();
            properties.put(key, value);
        }
        return properties;
    }
    
    private void initInsertContext()
        throws SemanticAnalyzerException
    {
        InsertUserOperatorStatementContext insertContext = fullContext.getInsertContext();
        getAnalyzeContext().setOutputStreamName(insertContext.getStreamName());
        UsingStatementContext usingStatement = insertContext.getUsingContext();
        if (usingStatement == null)
        {
            return;
        }
        
        getAnalyzeContext().setInputStreamName(usingStatement.getStreamName());
        getAnalyzeContext().setOperatorName(usingStatement.getOperatorName());
        
        setDistributed(usingStatement);
        setParallelNumber(usingStatement);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        insertUserOperatorStatementAnalyzeContext = new InsertUserOperatorStatementAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected InsertUserOperatorStatementAnalyzeContext getAnalyzeContext()
    {
        return insertUserOperatorStatementAnalyzeContext;
    }
    
    private void setDistributed(UsingStatementContext usingStatement)
    {
        if (usingStatement.getDistributeContext() != null)
        {
            getAnalyzeContext().setDistributedByColumnName(usingStatement.getDistributeContext().getColumnName());
        }
    }
    
    private void setParallelNumber(UsingStatementContext usingStatement)
        throws SemanticAnalyzerException
    {
        if (usingStatement.getParallelNumber() != null)
        {
            String number = usingStatement.getParallelNumber().getNumber();
            Integer parallelNumber = ConstUtils.formatInt(number);
            getAnalyzeContext().setParallelNumber(parallelNumber);
        }
    }
    
}
