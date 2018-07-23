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

import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.InsertAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.InsertStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * insert语义分析
 * 
 */
public class InsertStatementAnalyzer extends BaseAnalyzer
{
    
    private InsertAnalyzeContext context = null;
    
    private InsertStatementContext insertContext;
    
    /**
     * <默认构造函数>
     */
    public InsertStatementAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        insertContext = (InsertStatementContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        String streamName = insertContext.getStreamName();
        context.setOutputStreamName(streamName);
        
        if (checkSchemaExists(streamName))
        {
            context.setOutputSchema(getSchemaByName(streamName));
        }
        else
        {
            context.setPipeStreamNotCreated(true);
        }
        
        analyzeSelectStatement();
        createOutputStream();
        return context;
    }
    
    private void analyzeSelectStatement()
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer selectAnalzyer =
            SemanticAnalyzerFactory.createAnalyzer(insertContext.getSelect(), getAllSchemas());
        
        if (selectAnalzyer instanceof SelectStatementAnalyzer)
        {
            ((SelectStatementAnalyzer)selectAnalzyer).setOutputStreamName(context.getOutputStreamName());
        }
        
        context.setSelectContext((SelectAnalyzeContext)selectAnalzyer.analyze());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        context = new InsertAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return context;
    }
    
    private void createOutputStream()
    {
        if (!context.isPipeStreamNotCreated())
        {
            return;
        }
        Schema outputSchema = context.getSelectContext().getSelectClauseContext().getOutputSchema();
        outputSchema.setId(context.getOutputStreamName());
        outputSchema.setName(context.getOutputStreamName());
        outputSchema.setStreamName(context.getOutputStreamName());
        context.setOutputSchema(outputSchema);
    }
}
