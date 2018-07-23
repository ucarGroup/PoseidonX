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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.PropertyExpressionWalker;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.PropertyValueExpressionWalker;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.AtomExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.BaseExpressionParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FieldExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectClauseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectItemContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.StreamAllColumnsContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * select子句语法分析
 *
 */
public class SelectClauseAnalyzer extends BaseAnalyzer
{
    private static final Logger LOG = LoggerFactory.getLogger(SelectClauseAnalyzer.class);
    
    private static final String DEFAULT_SCHEMA_NAME = "tmpschema";
    
    private SelectClauseAnalyzeContext selectAnalyzeContext;
    
    private SelectClauseContext selectClauseParseConext;
    
    private List<BaseExpressionParseContext> selectExpressions = null;
    
    private Schema selectOutputSchema;
    
    /**
     * <默认构造函数>
     *
     */
    public SelectClauseAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        selectClauseParseConext = (SelectClauseContext)parseContext;
        selectExpressions = Lists.newArrayList();
        selectOutputSchema = new Schema(DEFAULT_SCHEMA_NAME);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        parseSelectItems();
        createSelectExpressionDescs();
        selectAnalyzeContext.setOutputSchema(selectOutputSchema);
        return selectAnalyzeContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        selectAnalyzeContext = new SelectClauseAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return selectAnalyzeContext;
    }
    
    private void parseSelectItems()
        throws SemanticAnalyzerException
    {
        selectAnalyzeContext.setDistinct(selectClauseParseConext.isDistinct());
        
        for (SelectItemContext selectItem : selectClauseParseConext.getSelectItems())
        {
            parseSelectItem(selectItem);
        }
    }
    
    private void parseSelectItem(SelectItemContext selectItem)
        throws SemanticAnalyzerException
    {
        boolean selectStar = selectItem.getExpression().getAllColumns() != null;
        if (selectStar)
        {
            parseStarExpression(selectItem);
        }
        else
        {
            parseExpression(selectItem);
        }
    }
    
    private void parseExpression(SelectItemContext selectItem)
        throws SemanticAnalyzerException
    {
        selectExpressions.add(selectItem.getExpression().getExpression());
        addColumnForSelectItem(selectItem);
    }
    
    private void createSelectExpressionDescs()
        throws SemanticAnalyzerException
    {
        for (BaseExpressionParseContext exps : selectExpressions)
        {
            selectAnalyzeContext.addExpressionDesc(exps.createExpressionDesc(getAllSchemas()));
        }
    }
    
    private void addColumnForSelectItem(SelectItemContext selectItem)
        throws SemanticAnalyzerException
    {
        if (selectItem.getExpression().getAlias() != null)
        {
            addColumnsForAlias(selectItem);
        }
        else
        {
            addColumnForExpression(selectItem);
        }
    }
    
    private void addColumnForExpression(SelectItemContext selectItem)
    {
        ExpressionContext expression = selectItem.getExpression().getExpression();
        String newColName = createColumnNameForPropertyValueExpression(expression);
        if (newColName == null)
        {
            newColName = createNewName(selectOutputSchema);
        }
        
        selectOutputSchema.addCol(new Column(newColName, null));
    }
    
    private void addColumnsForAlias(SelectItemContext selectItem)
        throws SemanticAnalyzerException
    {
        for (String alia : selectItem.getExpression().getAlias().getAlias())
        {
            if (selectOutputSchema.isAttributeExist(alia))
            {
                alia = renameNewName(selectOutputSchema, alia);
            }
            //输出schema的类型，后面完成了表达式解析之后再计算
            selectOutputSchema.addCol(new Column(alia, null));
            
            //设置原始列对应schema的列别名
            resetColumnAlias(selectItem, alia);
        }
    }
    
    /*
     * 如果是propertyValueExpression，则将原始的列名称赋给现在的列名称
     */
    private String createColumnNameForPropertyValueExpression(ExpressionContext exp)
    {
        PropertyValueExpressionWalker walker = new PropertyValueExpressionWalker();
        exp.walk(walker);
        String newColName = walker.getColumnName();
        if (newColName != null)
        {
            if (selectOutputSchema.isAttributeExist(newColName))
            {
                newColName = renameNewName(selectOutputSchema, newColName);
            }
        }
        return newColName;
    }
    
    private void resetColumnAlias(SelectItemContext selectItem, String alia)
        throws SemanticAnalyzerException
    {
        PropertyExpressionWalker walker = new PropertyExpressionWalker();
        selectItem.getExpression().getExpression().walk(walker);
        
        String columnName = walker.getColumnName();
        String streamName = walker.getStreamName();
        if (columnName == null)
        {
            return;
        }
        
        if (streamName != null)
        {
            Schema schema = getSchemaByName(streamName);
            List<Column> columns = BaseAnalyzer.getAttributeByName(columnName, schema, getAllSchemas());
            validateMoreColumnError(columns);
            validateNonColumns(columnName, schema, columns);
            columns.get(0).setAlias(alia);
        }
        else
        {
            List<Column> columns = BaseAnalyzer.getAttributeByName(columnName, getAllSchemas());
            validateMoreColumnError(columns);
            validateNonColumns(columnName, columns);
            columns.get(0).setAlias(alia);
        }
    }
    
    private void validateNonColumns(String columnName, List<Column> columns)
        throws SemanticAnalyzerException
    {
        if (columns.size() == 0)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_NO_COLUMN_ALLSTREAM, columnName);
            LOG.error("Can't find column in streams.", exception);
            
            throw exception;
        }
    }
    
    private void validateNonColumns(String columnName, Schema schema, List<Column> columns)
        throws SemanticAnalyzerException
    {
        if (columns.size() == 0)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_NO_COLUMN, columnName, schema.getId());
            LOG.error("Can't find column in stream.", exception);
            
            throw exception;
        }
    }
    
    private void validateMoreColumnError(List<Column> columns)
        throws SemanticAnalyzerException
    {
        if (columns.size() > 1)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_DUPLICATE_COLUMN_ALLSTREAM, columns.get(0)
                    .getName());
            LOG.error("One column in multi stream.", exception);
            
            throw exception;
        }
    }
    
    private void parseStarExpression(SelectItemContext selectItem)
        throws SemanticAnalyzerException
    {
        if (isStarWithStreamNameExpression(selectItem.getExpression().getAllColumns()))
        {
            parseStarWithStreamNameExpression(selectItem);
        }
        else
        {
            parseStarWithOutStreamNameExpression();
        }
    }
    
    private void parseStarWithOutStreamNameExpression()
    {
        for (Schema schema : getAllSchemas())
        {
            for (Column column : schema.getCols())
            {
                String colName = column.getName();
                if (selectOutputSchema.isAttributeExist(colName))
                {
                    colName = renameNewName(selectOutputSchema, colName);
                }
                selectOutputSchema.addCol(new Column(colName, null));
                selectExpressions.add(createFieldExpression(schema.getId(), column.getName()));
            }
        }
    }
    
    private void parseStarWithStreamNameExpression(SelectItemContext selectItem)
        throws SemanticAnalyzerException
    {
        String schemaName = selectItem.getExpression().getAllColumns().getStreamName();
        List<Column> attrsInSchema = getAttributes(getSchemaByName(schemaName));
        for (int i = 0; i < attrsInSchema.size(); i++)
        {
            String colName = attrsInSchema.get(i).getName();
            addColumnForOutputSchema(colName);
            selectExpressions.add(createFieldExpression(schemaName, colName));
        }
    }
    
    private void addColumnForOutputSchema(final String colName)
    {
        String newColName = colName;
        if (selectOutputSchema.isAttributeExist(newColName))
        {
            newColName = renameNewName(selectOutputSchema, newColName);
        }
        selectOutputSchema.addCol(new Column(newColName, null));
    }
    
    private FieldExpressionContext createFieldExpression(String schemaName, String columnName)
    {
        AtomExpressionContext atomExp = craeteAtomExpression(columnName);
        FieldExpressionContext fexp = new FieldExpressionContext();
        fexp.setStreamNameOrAlias(schemaName);
        fexp.setAtomExpression(atomExp);
        return fexp;
    }
    
    private AtomExpressionContext craeteAtomExpression(String columnName)
    {
        AtomExpressionContext atomExp = new AtomExpressionContext();
        atomExp.setColumnName(columnName);
        return atomExp;
    }
    
    /**
     * 是否包含了Schema名称
     *
     */
    private boolean isStarWithStreamNameExpression(StreamAllColumnsContext allColumns)
    {
        return allColumns.getStreamName() != null;
    }
    
}
