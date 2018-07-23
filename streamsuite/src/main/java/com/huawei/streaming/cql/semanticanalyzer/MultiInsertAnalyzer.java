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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FromClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectWithOutFromAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.MultiInsertContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * 多级insert语义分析
 *
 */
public class MultiInsertAnalyzer extends BaseAnalyzer
{
    private MultiInsertAnalyzeContext context = null;
    
    private MultiInsertContext multiInsert;
    
    private FromClauseAnalyzeContext fromAnalyzeContext;
    
    /**
     * <默认构造函数>
     *
     */
    public MultiInsertAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        multiInsert = (MultiInsertContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        analyzeInsertClause();
        analyzeSelectStatement();
        createOutputStream();
        return context;
    }
    
    private void analyzeInsertClause()
        throws SemanticAnalyzerException
    {
        String streamName = multiInsert.getOutputStream();
        context.setOutputStreamName(streamName);
        
        if (checkSchemaExists(streamName))
        {
            context.setOutputSchema(getSchemaByName(streamName));
        }
        else
        {
            context.setPipeStreamNotCreated(true);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        context = new MultiInsertAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return context;
    }
    
    private void analyzeSelectStatement()
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer selectAnalzyer =
            SemanticAnalyzerFactory.createAnalyzer(multiInsert.getSelects(), getAllSchemas());
        SelectWithOutFromAnalyzer sAnalyzer = (SelectWithOutFromAnalyzer)selectAnalzyer;
        sAnalyzer.setFromAnalyzeContext(getFromAnalyzeContext());
        context.setSelectContext((SelectWithOutFromAnalyzeContext)selectAnalzyer.analyze());
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
    
    public FromClauseAnalyzeContext getFromAnalyzeContext()
    {
        return fromAnalyzeContext;
    }
    
    public void setFromAnalyzeContext(FromClauseAnalyzeContext fromAnalyzeContext)
    {
        this.fromAnalyzeContext = fromAnalyzeContext;
    }
    
}
