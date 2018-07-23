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

import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.exception.ParseException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.expressioncreater.ExpressionCreatorFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FilterClauseAnalzyeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FromClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.LimitClauseAnalzyeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.OrderByClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectWithOutFromAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.MultiSelectContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectItemContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.StreamAllColumnsContext;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.expression.IExpression;

/**
 * 多级insert语义分析
 *
 */
public class SelectWithOutFromAnalyzer extends BaseAnalyzer
{
    private static final Logger LOG = LoggerFactory.getLogger(SelectWithOutFromAnalyzer.class);
    
    private SelectWithOutFromAnalyzeContext context = null;
    
    private MultiSelectContext selectContext;
    
    private FromClauseAnalyzeContext fromAnalyzeContext;
    
    /**
     * <默认构造函数>
     *
     */
    public SelectWithOutFromAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        selectContext = (MultiSelectContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SelectWithOutFromAnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        selectAnalyzer();
        resetOutputColumnTypes();
        whereAnalyzer();
        groupbyAnalyzer();
        havingAnalyzer();
        orderbyAnalyzer();
        limitAnalyzer();
        return context;
    }
    
    public FromClauseAnalyzeContext getFromAnalyzeContext()
    {
        return fromAnalyzeContext;
    }
    
    public void setFromAnalyzeContext(FromClauseAnalyzeContext fromAnalyzeContext)
    {
        this.fromAnalyzeContext = fromAnalyzeContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        context = new SelectWithOutFromAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return context;
    }
    
    private void whereAnalyzer()
        throws SemanticAnalyzerException
    {
        if (selectContext.getWhere() == null)
        {
            return;
        }
        
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(selectContext.getWhere(), getInputSchemas());
        context.setWhereClauseContext((FilterClauseAnalzyeContext)analyzer.analyze());
    }
    
    private void limitAnalyzer()
        throws SemanticAnalyzerException
    {
        
        if (selectContext.getLimit() == null)
        {
            return;
        }
        
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(selectContext.getLimit(), null);
        context.setLimitClauseContext((LimitClauseAnalzyeContext)analyzer.analyze());
    }

    private void orderbyAnalyzer()
        throws SemanticAnalyzerException
    {
        if (selectContext.getOrderby() == null)
        {
            return;
        }

        OrderByClauseAnalyzer analyzer =
         (OrderByClauseAnalyzer)SemanticAnalyzerFactory.createAnalyzer(selectContext.getOrderby(), getMixedSchemas());
        analyzer.addSelectItems(getAllSelectItems());
        context.setOrderbyClauseContext((OrderByClauseAnalyzeContext)analyzer.analyze());
    }
    
    private void havingAnalyzer()
        throws SemanticAnalyzerException
    {
        if (selectContext.getHaving() == null)
        {
            return;
        }
        
        FilterClauseAnalyzer analyzer =
            (FilterClauseAnalyzer)SemanticAnalyzerFactory.createAnalyzer(selectContext.getHaving(), getMixedSchemas());
        analyzer.addSelectItems(getAllSelectItems());
        context.setHavingClauseContext((FilterClauseAnalzyeContext)analyzer.analyze());
    }
    
    private void groupbyAnalyzer()
        throws SemanticAnalyzerException
    {
        if (selectContext.getGroupby() == null)
        {
            return;
        }
        
        SemanticAnalyzer analyzer =
            SemanticAnalyzerFactory.createAnalyzer(selectContext.getGroupby(), getInputSchemas());
        context.setGroupbyClauseContext((SelectClauseAnalyzeContext)analyzer.analyze());
    }
    
    private void selectAnalyzer()
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer analyzer =
            SemanticAnalyzerFactory.createAnalyzer(selectContext.getSelect(), getInputSchemas());
        context.setSelectClauseContext((SelectClauseAnalyzeContext)analyzer.analyze());
    }
    
    private List<Schema> getInputSchemas()
        throws SemanticAnalyzerException
    {
        if (getFromAnalyzeContext() == null)
        {
            LOG.error("'{}' was not parsed.", "From clause");
            SemanticAnalyzerException exception = new SemanticAnalyzerException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        return getFromAnalyzeContext().getInputSchemas();
    }
    
    /*
     * 只需要对count之类的表达式进行替换
     * *号表达式不需要替换
     */
    private List<SelectItemContext> getAllSelectItems()
        throws SemanticAnalyzerException
    {
        if (context.getSelectClauseContext() == null)
        {
            LOG.error("'{}' was not parsed.", "Select clause");
            SemanticAnalyzerException exception = new SemanticAnalyzerException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        List<SelectItemContext> selectItems = Lists.newArrayList();
        
        for (SelectItemContext selectItem : selectContext.getSelect().getSelectItems())
        {
            StreamAllColumnsContext allColumns = selectItem.getExpression().getAllColumns();
            if (allColumns != null)
            {
                List<SelectItemContext> allColExpressions = createNewExpressionsForAllInputSchemas(allColumns);
                selectItems.addAll(allColExpressions);
            }
            else
            {
                selectItems.add(selectItem);
            }
        }
        return selectItems;
    }
    
    private List<SelectItemContext> createNewExpressionsForAllInputSchemas(StreamAllColumnsContext allColumnContext)
        throws SemanticAnalyzerException
    {
        List<SelectItemContext> newExpressions = Lists.newArrayList();
        
        //a.*
        if (allColumnContext.getStreamName() != null)
        {
            Schema schema = BaseAnalyzer.getSchemaByName(allColumnContext.getStreamName(), getInputSchemas());
            newExpressions.addAll(createNewExpressionForSchema(schema));
            return newExpressions;
        }
        
        //*
        List<Schema> schemas = getInputSchemas();
        for (Schema schema : schemas)
        {
            newExpressions.addAll(createNewExpressionForSchema(schema));
        }
        return newExpressions;
    }
    
    private List<SelectItemContext> createNewExpressionForSchema(Schema schema)
        throws SemanticAnalyzerException
    {
        List<SelectItemContext> asts = Lists.newArrayList();
        IParser parser = ParserFactory.createApplicationParser();
        for (Column column : schema.getCols())
        {
            asts.add(createSelectItemContext(parser, column));
        }
        return asts;
    }
    
    private SelectItemContext createSelectItemContext(IParser parser, Column column)
        throws ParseException
    {
        SelectExpressionContext selectExp = createSelectExpression(parser, column);
        SelectItemContext item = new SelectItemContext();
        item.setExpression(selectExp);
        return item;
    }
    
    private SelectExpressionContext createSelectExpression(IParser parser, Column column)
        throws ParseException
    {
        ExpressionContext exp = (ExpressionContext)parser.parse(column.getName());
        SelectExpressionContext selectExp = new SelectExpressionContext();
        selectExp.setExpression(exp);
        return selectExp;
    }
    
    /**
     * 获取混合的schema
     * 该schema，最后面一定是输入schema
     * 第一位是输出schema
     *
     */
    private List<Schema> getMixedSchemas()
        throws SemanticAnalyzerException
    {
        if (context.getSelectClauseContext() == null)
        {
            LOG.error("'{}' was not parsed.", "Select clause");
            SemanticAnalyzerException exception = new SemanticAnalyzerException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        List<Schema> rs = Lists.newArrayList();
        rs.add(context.getSelectClauseContext().getOutputSchema());
        rs.addAll(getInputSchemas());
        return rs;
    }
    
    private void resetOutputColumnTypes()
        throws SemanticAnalyzerException
    {
        SelectClauseAnalyzeContext selectClause = context.getSelectClauseContext();
        
        Schema outputSchema = selectClause.getOutputSchema();
        if (outputSchema.getCols().size() != selectClause.getExpdes().size())
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_NOTSAME_COLUMNS,
                    String.valueOf(selectClause.getExpdes().size()), String.valueOf(outputSchema.getCols().size()));
            LOG.error("Select column not match ouput schema column.", exception);
            
            throw exception;
        }
        resetColumnTypes(selectClause, outputSchema);
    }
    
    private void resetColumnTypes(SelectClauseAnalyzeContext selectClause, Schema outputSchema)
        throws SemanticAnalyzerException
    {
        for (int i = 0; i < outputSchema.getCols().size(); i++)
        {
            Column c = outputSchema.getCols().get(i);
            ExpressionDescribe expdesc = selectClause.getExpdes().get(i);
            IExpression exp = createExpression(expdesc);
            if (exp != null)
            {
                c.setType(exp.getType().getName());
                
            }
        }
    }
    
    private IExpression createExpression(ExpressionDescribe exp)
        throws SemanticAnalyzerException
    {
        try
        {
            return ExpressionCreatorFactory.createExpression(exp, new HashMap<String, String>());
        }
        catch (ExecutorException e)
        {
            throw SemanticAnalyzerException.wrapStreamingException(e);
        }
    }
    
}
