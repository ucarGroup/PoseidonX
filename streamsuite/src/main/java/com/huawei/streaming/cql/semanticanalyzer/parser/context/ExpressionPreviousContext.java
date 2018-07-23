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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.huawei.streaming.cql.CQLConst;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ConstExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionPreviousDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.exception.ErrorCode;

/**
 * prvious表达式解析内容
 *
 */
public class ExpressionPreviousContext extends BaseExpressionParseContext
{
    private static final Logger LOG = LoggerFactory.getLogger(ExpressionPreviousContext.class);
    
    private List<BaseExpressionParseContext> expressions;
    
    /**
     * <默认构造函数>
     */
    public ExpressionPreviousContext()
    {
        expressions = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("PREVIOUS(");
        sb.append(Joiner.on(", ").join(expressions));
        sb.append(")");
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpressions(walker, expressions);
    }
    
    public List<BaseExpressionParseContext> getExpressions()
    {
        return expressions;
    }
    
    public void setExpressions(List<BaseExpressionParseContext> expressions)
    {
        this.expressions = expressions;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        List<Integer> replacedIndex = foundIndexsInChilds(replacer);
        replace(replacedIndex, replacer);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        if (expressions.size() < CQLConst.I_2)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.FUNCTION_PREVIOUS_PROPERTYVALUE_EXPRSSION);
            LOG.error(ErrorCode.FUNCTION_PREVIOUS_PROPERTYVALUE_EXPRSSION.getFullMessage(), exception);
            
            throw exception;
        }
        
        List<PropertyValueExpressionDesc> previousColumns = Lists.newArrayList();
        
        for (int i = 1; i < expressions.size(); i++)
        {
            BaseExpressionParseContext exp = expressions.get(i);
            validatePreviouColumn(exp.createExpressionDesc(getSchemas()));
            previousColumns.add((PropertyValueExpressionDesc)exp.createExpressionDesc(getSchemas()));
        }
        validatePreviouNumber(expressions.get(0).createExpressionDesc(getSchemas()));
        
        FunctionPreviousDesc desc = new FunctionPreviousDesc();
        desc.setPreviouCols(previousColumns);
        desc.setPreviouNumber((ConstExpressionDesc)expressions.get(0).createExpressionDesc(getSchemas()));
        return desc;
    }
    
    private void replace(List<Integer> replacedIndex, ParseContextReplacer replacer)
    {
        BaseExpressionParseContext replacedContext = replacer.createReplaceParseContext();
        for (Integer index : replacedIndex)
        {
            expressions.set(index, replacedContext);
        }
    }
    
    private List<Integer> foundIndexsInChilds(ParseContextReplacer replacer)
    {
        List<Integer> replacedIndex = Lists.newArrayList();
        for (int i = 0; i < expressions.size(); i++)
        {
            BaseExpressionParseContext child = expressions.get(i);
            if (replacer.isChildsReplaceable(child))
            {
                replacedIndex.add(i);
            }
            else
            {
                child.walkChildAndReplace(replacer);
            }
        }
        return replacedIndex;
    }
    
    private void validatePreviouColumn(ExpressionDescribe preCol)
        throws SemanticAnalyzerException
    {
        if (!(preCol instanceof PropertyValueExpressionDesc))
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.FUNCTION_PREVIOUS_PROPERTYVALUE_EXPRSSION);
            LOG.error(exception.getMessage(), exception);
            
            throw exception;
        }
    }
    
    private void validatePreviouNumber(ExpressionDescribe preNum)
        throws SemanticAnalyzerException
    {
        if (preNum instanceof ConstExpressionDesc)
        {
            ConstExpressionDesc constPreNum = (ConstExpressionDesc)preNum;
            if (!(constPreNum.getConstValue().getClass() == Integer.class))
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.FUNCTION_PREVIOUS_PROPERTYVALUE_EXPRSSION);
                LOG.error(ErrorCode.FUNCTION_PREVIOUS_PROPERTYVALUE_EXPRSSION.getFullMessage(), exception);
                
                throw exception;
            }
        }
    }
}
