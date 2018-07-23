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

package com.huawei.streaming.cql.semanticanalyzer.parser.context;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.BaseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 原子表达式解析内容
 *
 */
public class AtomExpressionContext extends BaseExpressionParseContext
{
    private static final Logger LOG = LoggerFactory.getLogger(AtomExpressionContext.class);
    
    private BaseExpressionParseContext constant;
    
    private BaseExpressionParseContext function;
    
    private BaseExpressionParseContext castExpression;
    
    private BaseExpressionParseContext caseExpression;
    
    private BaseExpressionParseContext whenExpression;
    
    private String columnName;
    
    private BaseExpressionParseContext expressionWithLaparen;
    
    private BaseExpressionParseContext previous;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (constant != null)
        {
            sb.append(constant.toString());
        }
        if (function != null)
        {
            sb.append(function.toString());
        }
        if (castExpression != null)
        {
            sb.append(castExpression.toString());
        }
        if (caseExpression != null)
        {
            sb.append(caseExpression.toString());
        }
        if (whenExpression != null)
        {
            sb.append(whenExpression.toString());
        }
        if (columnName != null)
        {
            sb.append(columnName);
        }
        if (expressionWithLaparen != null)
        {
            sb.append(expressionWithLaparen);
        }
        if (previous != null)
        {
            sb.append(previous);
        }
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, constant);
        walkExpression(walker, function);
        walkExpression(walker, castExpression);
        walkExpression(walker, caseExpression);
        walkExpression(walker, whenExpression);
        walkExpression(walker, expressionWithLaparen);
        walkExpression(walker, previous);
    }
    
    public BaseExpressionParseContext getConstant()
    {
        return constant;
    }
    
    public void setConstant(BaseExpressionParseContext constant)
    {
        this.constant = constant;
    }
    
    public BaseExpressionParseContext getFunction()
    {
        return function;
    }
    
    public void setFunction(BaseExpressionParseContext function)
    {
        this.function = function;
    }
    
    public BaseExpressionParseContext getCastExpression()
    {
        return castExpression;
    }
    
    public void setCastExpression(BaseExpressionParseContext castExpression)
    {
        this.castExpression = castExpression;
    }
    
    public BaseExpressionParseContext getCaseExpression()
    {
        return caseExpression;
    }
    
    public void setCaseExpression(BaseExpressionParseContext caseExpression)
    {
        this.caseExpression = caseExpression;
    }
    
    public BaseExpressionParseContext getWhenExpression()
    {
        return whenExpression;
    }
    
    public void setWhenExpression(BaseExpressionParseContext whenExpression)
    {
        this.whenExpression = whenExpression;
    }
    
    public BaseExpressionParseContext getExpressionWithLaparen()
    {
        return expressionWithLaparen;
    }
    
    public void setExpressionWithLaparen(BaseExpressionParseContext expressionWithLaparen)
    {
        this.expressionWithLaparen = expressionWithLaparen;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        walkChildAndReplaceConstant(replacer);
        walkChildAndReplaceFunction(replacer);
        walkChildAndReplaceCast(replacer);
        walkChildAndReplaceCase(replacer);
        walkChildAndReplaceWhen(replacer);
        walkChildAndReplaceExpWithLaparen(replacer);
        walkChildAndReplacePrevious(replacer);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        if (constant != null)
        {
            return constant.createExpressionDesc(getSchemas());
        }
        if (function != null)
        {
            return function.createExpressionDesc(getSchemas());
        }
        if (castExpression != null)
        {
            return castExpression.createExpressionDesc(getSchemas());
        }
        if (caseExpression != null)
        {
            return caseExpression.createExpressionDesc(getSchemas());
        }
        if (whenExpression != null)
        {
            return whenExpression.createExpressionDesc(getSchemas());
        }
        
        if (columnName != null)
        {
            int index = foundIndex();
            Schema schema = null;
            
            if (getLeftExpression() != null)
            {
                if (getLeftExpression() instanceof FieldExpressionContext)
                {
                    FieldExpressionContext fieldExp = (FieldExpressionContext)getLeftExpression();
                    if (fieldExp.getStreamNameOrAlias() != null)
                    {
                        schema = BaseAnalyzer.getSchemaByName(fieldExp.getStreamNameOrAlias(), getSchemas());
                    }
                }
            }
            List<Column> attrs = getAttributeByName(columnName, schema);
            
            validateColumns(attrs);
            if (schema != null)
            {
                return new PropertyValueExpressionDesc(attrs.get(0), schema.getId(), index);
            }
            List<String> schemas = getSchemaNameByAttrName(columnName);
            return new PropertyValueExpressionDesc(attrs.get(0), schemas.get(0), index);
        }
        
        if (previous != null)
        {
            return previous.createExpressionDesc(getSchemas());
        }
        
        return expressionWithLaparen.createExpressionDesc(getSchemas());
    }
    
    public String getColumnName()
    {
        return columnName;
    }
    
    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }
    
    public BaseExpressionParseContext getPrevious()
    {
        return previous;
    }
    
    public void setPrevious(BaseExpressionParseContext previous)
    {
        this.previous = previous;
    }
    
    private void walkChildAndReplacePrevious(ParseContextReplacer replacer)
    {
        if (previous == null)
        {
            return;
        }
        
        if (replacer.isChildsReplaceable(previous))
        {
            previous = replacer.createReplaceParseContext();
        }
        else
        {
            previous.walkChildAndReplace(replacer);
        }
        
    }
    
    private void walkChildAndReplaceExpWithLaparen(ParseContextReplacer replacer)
    {
        if (expressionWithLaparen == null)
        {
            return;
        }
        if (replacer.isChildsReplaceable(expressionWithLaparen))
        {
            expressionWithLaparen = replacer.createReplaceParseContext();
        }
        else
        {
            expressionWithLaparen.walkChildAndReplace(replacer);
        }
    }
    
    private void walkChildAndReplaceWhen(ParseContextReplacer replacer)
    {
        if (whenExpression == null)
        {
            return;
        }
        
        if (replacer.isChildsReplaceable(whenExpression))
        {
            whenExpression = replacer.createReplaceParseContext();
        }
        else
        {
            whenExpression.walkChildAndReplace(replacer);
        }
    }
    
    private void walkChildAndReplaceCase(ParseContextReplacer replacer)
    {
        if (caseExpression == null)
        {
            return;
        }
        
        if (replacer.isChildsReplaceable(caseExpression))
        {
            caseExpression = replacer.createReplaceParseContext();
        }
        else
        {
            caseExpression.walkChildAndReplace(replacer);
        }
    }
    
    private void walkChildAndReplaceCast(ParseContextReplacer replacer)
    {
        if (castExpression == null)
        {
            return;
        }
        
        if (replacer.isChildsReplaceable(castExpression))
        {
            castExpression = replacer.createReplaceParseContext();
        }
        else
        {
            castExpression.walkChildAndReplace(replacer);
        }
    }
    
    private void walkChildAndReplaceFunction(ParseContextReplacer replacer)
    {
        if (function == null)
        {
            return;
        }
        
        if (replacer.isChildsReplaceable(function))
        {
            function = replacer.createReplaceParseContext();
        }
        else
        {
            function.walkChildAndReplace(replacer);
        }
    }
    
    private void walkChildAndReplaceConstant(ParseContextReplacer replacer)
    {
        if (constant == null)
        {
            return;
        }
        
        if (replacer.isChildsReplaceable(constant))
        {
            constant = replacer.createReplaceParseContext();
        }
        else
        {
            constant.walkChildAndReplace(replacer);
        }
    }
    
    private int foundIndex()
        throws SemanticAnalyzerException
    {
        int index = 0;
        
        if (getLeftExpression() != null)
        {
            if (getLeftExpression() instanceof FieldExpressionContext)
            {
                FieldExpressionContext fieldExp = (FieldExpressionContext)getLeftExpression();
                if (fieldExp.getStreamNameOrAlias() != null)
                {
                    index = getIndexInSchemas(fieldExp.getStreamNameOrAlias());
                }
            }
        }
        return index;
    }
    
    private void validateColumns(List<Column> attrs)
        throws SemanticAnalyzerException
    {
        if (attrs.size() > 1)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_DUPLICATE_COLUMN_ALLSTREAM, columnName);
            LOG.error(ErrorCode.SEMANTICANALYZE_DUPLICATE_COLUMN_ALLSTREAM.getFullMessage(columnName), exception);
            throw exception;
        }
        
        if (attrs.size() == 0)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_NO_COLUMN_ALLSTREAM, columnName);
            LOG.error(ErrorCode.SEMANTICANALYZE_NO_COLUMN_ALLSTREAM.getFullMessage(columnName), exception);
            throw exception;
        }
    }
}
