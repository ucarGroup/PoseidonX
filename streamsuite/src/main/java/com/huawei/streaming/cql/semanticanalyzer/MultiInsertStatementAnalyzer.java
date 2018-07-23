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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.DataSourceQueryArgumentsAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FromClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.MultiInsertStatementAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.ParallelClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.DatasourceBodyDesc;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.DataSourceBodyContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.JoinRightBodyContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.MultiInsertContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.MultiInsertStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 多级insert语义分析
 *
 */
public class MultiInsertStatementAnalyzer extends BaseAnalyzer
{
    private static final Logger LOG = LoggerFactory.getLogger(MultiInsertStatementAnalyzer.class);
    
    private MultiInsertStatementAnalyzeContext context = null;
    
    private MultiInsertStatementContext multiInsertContext;
    
    private boolean isMultiInsert = true;
    
    /**
     * <默认构造函数>
     *
     */
    public MultiInsertStatementAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        multiInsertContext = (MultiInsertStatementContext)parseContext;
        if (multiInsertContext.getInserts().size() == 1)
        {
            isMultiInsert = false;
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        fromAnalyzer();
        parallelAnalyzer();
        
        //这里先按照dataSource的方式解析，之后再检测，防止出现误解析的情况
        dataSourceQueryArgumentsAnalyzer();
        for (MultiInsertContext select : multiInsertContext.getInserts())
        {
            SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(select, getAllSchemas());
            MultiInsertAnalyzer mAnalzyer = (MultiInsertAnalyzer)analyzer;
            mAnalzyer.setFromAnalyzeContext(context.getFrom());
            context.getMultiSelectBodyAnalyzeContexts().add((MultiInsertAnalyzeContext)mAnalzyer.analyze());
        }
        validateSelectStatement();
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        context = new MultiInsertStatementAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return context;
    }
    
    private void parallelAnalyzer()
        throws SemanticAnalyzerException
    {
        if (multiInsertContext.getParallel() == null)
        {
            return;
        }
        
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(multiInsertContext.getParallel(), null);
        context.setParallelClause((ParallelClauseAnalyzeContext)analyzer.analyze());
    }
    
    private void fromAnalyzer()
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer analyzer =
            SemanticAnalyzerFactory.createAnalyzer(multiInsertContext.getFrom(), getAllSchemas());
        context.setFrom((FromClauseAnalyzeContext)analyzer.analyze());
    }
    
    private void validateSelectStatement()
        throws SemanticAnalyzerException
    {
        if (!isMultiInsert)
        {
            return;
        }
        
        /*
         *  多级输入语句中，不允许出现多个输入流，比如join、combine等
         *  不允许出现聚合操作。
         */
        multiInputStreamValidate();
        windowValidate();
        aggregateValidate();
        dataSourceValidate();
    }
    
    private void dataSourceValidate()
        throws SemanticAnalyzerException
    {
        if (context.getFrom().getJoinexpression() != null)
        {
            if (context.getFrom().getJoinexpression().getRightExpression() instanceof DatasourceBodyDesc)
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_MULTIINSERT_DATASOURCE);
                LOG.error("DataSource appear in multi insert clause.", exception);
                
                throw exception;
            }
        }
    }
    
    private void aggregateValidate()
        throws SemanticAnalyzerException
    {
        for (MultiInsertAnalyzeContext insert : context.getMultiSelectBodyAnalyzeContexts())
        {
            if (insert.getSelectContext().getGroupbyClauseContext() != null)
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_MULTIINSERT_GROUPBY);
                LOG.error("Groupby appear in multi insert clause.", exception);
                
                throw exception;
            }
            
        }
    }
    
    private void windowValidate()
        throws SemanticAnalyzerException
    {
        Map<String, Window> windows = context.getFrom().getWindows();
        for (Map.Entry<String, Window> et : windows.entrySet())
        {
            if (et.getValue() != null)
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_MULTIINSERT_WINDOW);
                LOG.error("Window appear in multi insert clause.", exception);
                
                throw exception;
            }
        }
    }
    
    private void multiInputStreamValidate()
        throws SemanticAnalyzerException
    {
        if (context.getFrom().getInputStreams().size() != 1)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_MULTIINSERT_JOIN);
            LOG.error("Too many input stream appear in multi insert clause.", exception);
            
            throw exception;
        }
    }
    
    private void dataSourceQueryArgumentsAnalyzer()
        throws SemanticAnalyzerException
    {
        DataSourceBodyContext datasource = getDataSource();
        if (datasource == null)
        {
            return;
        }
        
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(datasource, getInputSchemas());
        DataSourceQueryArgumentsAnalyzeContext dsContext = (DataSourceQueryArgumentsAnalyzeContext)analyzer.analyze();
        resetDataSourceArguments(dsContext);
    }
    
    private void resetDataSourceArguments(DataSourceQueryArgumentsAnalyzeContext dsContext)
    {
        if (!validateFromAnalzyeContext(context))
        {
            return;
        }
        
        FromClauseAnalyzeContext clauseContext = context.getFrom();
        DatasourceBodyDesc dsDesc = (DatasourceBodyDesc)clauseContext.getJoinexpression().getRightExpression();
        dsDesc.setQueryArguments(dsContext.getQueryArguments());
    }
    
    private boolean validateFromAnalzyeContext(AnalyzeContext parseContext)
    {
        if (!(parseContext instanceof SelectAnalyzeContext))
        {
            return false;
        }
        
        FromClauseAnalyzeContext clauseContext = context.getFrom();
        
        if (clauseContext.getCombineConditions().size() != 0)
        {
            return false;
        }
        
        if (clauseContext.getJoinexpression() == null)
        {
            return false;
        }
        
        return clauseContext.getJoinexpression().getRightExpression() instanceof DatasourceBodyDesc;
    }
    
    private DataSourceBodyContext getDataSource()
    {
        List<JoinRightBodyContext> rights = multiInsertContext.getFrom().getSourceContext().getJoinRightBody();
        if (rights != null && rights.size() != 0)
        {
            if (rights.get(0).getRightStream().getDataSoruceBody() != null)
            {
                return rights.get(0).getRightStream().getDataSoruceBody();
            }
        }
        return null;
    }
    
    private List<Schema> getInputSchemas()
        throws SemanticAnalyzerException
    {
        if (context.getFrom() == null)
        {
            LOG.error("'{}' was not parsed.", "From clause");
            SemanticAnalyzerException exception = new SemanticAnalyzerException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        return context.getFrom().getInputSchemas();
    }
    
}
